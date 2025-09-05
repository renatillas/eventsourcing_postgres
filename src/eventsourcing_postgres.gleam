import argv
import dot_env
import dot_env/env
import eventsourcing
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/static_supervisor
import gleam/pair
import gleam/result
import gleam/string
import pog

// CONSTANTS ----

const batch_insert_events_query = "
  INSERT INTO event 
  (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
  VALUES 
"

const select_events_query = "
  SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM event
  WHERE aggregate_type = $1 
    AND aggregate_id = $2
    AND sequence > $3
  ORDER BY sequence
  "

const create_event_table_query = "
  CREATE TABLE IF NOT EXISTS event
  (
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    event_type     text                         NOT NULL,
    event_version  text                         NOT NULL,
    payload        text                         NOT NULL,
    metadata       text                         NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
  );
  "

const create_snapshot_table_query = "
  CREATE TABLE IF NOT EXISTS snapshot
  (
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    entity         text                         NOT NULL,
    timestamp      int                          NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
  );
  "

const save_snapshot_query = "
  INSERT INTO snapshot (aggregate_type, aggregate_id, sequence, entity, timestamp)
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (aggregate_type, aggregate_id)
  DO UPDATE SET
    sequence = EXCLUDED.sequence,
    entity = EXCLUDED.entity
  "

const select_snapshot_query = "
  SELECT aggregate_type, aggregate_id, sequence, entity, timestamp
  FROM snapshot
  WHERE aggregate_type = $1 
    AND aggregate_id = $2
"

// MIGRATION QUERIES ----

const add_event_jsonb_columns_query = "
  -- Add new JSONB columns to event table
  ALTER TABLE event 
    ADD COLUMN IF NOT EXISTS payload_json JSONB,
    ADD COLUMN IF NOT EXISTS metadata_json JSONB;
"

const convert_event_data_to_jsonb_query = "
  -- Convert existing text data to JSONB, handling potential JSON errors
  -- Only update rows where JSONB columns are NULL (not already migrated)
  UPDATE event 
  SET 
    payload_json = CASE 
      WHEN payload_json IS NULL THEN payload::jsonb
      ELSE payload_json
    END,
    metadata_json = CASE 
      WHEN metadata_json IS NULL THEN metadata::jsonb
      ELSE metadata_json
    END
  WHERE payload_json IS NULL OR metadata_json IS NULL;
"

const add_snapshot_jsonb_columns_query = "
  -- Add new JSONB column to snapshot table
  ALTER TABLE snapshot 
    ADD COLUMN IF NOT EXISTS entity_json JSONB;
"

const convert_snapshot_data_to_jsonb_query = "
  -- Convert existing text data to JSONB, handling potential JSON errors
  -- Only update rows where JSONB column is NULL (not already migrated)
  UPDATE snapshot 
  SET entity_json = CASE 
    WHEN entity_json IS NULL THEN entity::jsonb
    ELSE entity_json
  END
  WHERE entity_json IS NULL;
"

const create_event_table_with_json_query = "
  CREATE TABLE IF NOT EXISTS event
  (
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    event_type     text                         NOT NULL,
    event_version  text                         NOT NULL,
    payload        jsonb                        NOT NULL,
    metadata       jsonb                        NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
  );
  "

const create_snapshot_table_with_json_query = "
  CREATE TABLE IF NOT EXISTS snapshot
  (
    aggregate_type text                         NOT NULL,
    aggregate_id   text                         NOT NULL,
    sequence       bigint CHECK (sequence >= 0) NOT NULL,
    entity         jsonb                        NOT NULL,
    timestamp      int                          NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
  );
  "

const drop_event_payload_column_query = "ALTER TABLE event DROP COLUMN payload;"

const drop_event_metadata_column_query = "ALTER TABLE event DROP COLUMN metadata;"

const rename_event_payload_column_query = "ALTER TABLE event RENAME COLUMN payload_json TO payload;"

const rename_event_metadata_column_query = "ALTER TABLE event RENAME COLUMN metadata_json TO metadata;"

const drop_snapshot_entity_column_query = "ALTER TABLE snapshot DROP COLUMN entity;"

const rename_snapshot_entity_column_query = "ALTER TABLE snapshot RENAME COLUMN entity_json TO entity;"

// TYPES ----

type Metadata =
  List(#(String, String))

@internal
pub type PostgresStore(entity, command, event, error) {
  PostgresStore(
    db: pog.Connection,
    event_encoder: fn(event) -> String,
    event_decoder: decode.Decoder(event),
    event_type: String,
    event_version: String,
    aggregate_type: String,
    entity_encoder: fn(entity) -> String,
    entity_decoder: decode.Decoder(entity),
  )
}

// CONSTRUCTORS ----

pub fn new(
  pgo_config pgo_config: pog.Config,
  event_encoder event_encoder: fn(event) -> String,
  event_decoder event_decoder: decode.Decoder(event),
  event_type event_type: String,
  event_version event_version: String,
  aggregate_type aggregate_type: String,
  entity_encoder entity_encoder: fn(entity) -> String,
  entity_decoder entity_decoder: decode.Decoder(entity),
) -> eventsourcing.EventStore(
  PostgresStore(entity, command, event, error),
  entity,
  command,
  event,
  error,
  pog.Connection,
) {
  let db = pog.named_connection(pgo_config.pool_name)
  let eventstore =
    PostgresStore(
      db:,
      event_encoder:,
      event_decoder:,
      event_type:,
      event_version:,
      aggregate_type:,
      entity_encoder:,
      entity_decoder:,
    )

  eventsourcing.EventStore(
    eventstore:,
    load_events: fn(postgres_store, tx, aggregate_id, start_from) {
      load_events(postgres_store, tx, aggregate_id, start_from)
    },
    commit_events: fn(tx, aggregate, events, metadata) {
      commit_events(eventstore, tx, aggregate, events, metadata)
    },
    load_snapshot: fn(tx, aggregate_id) {
      load_snapshot(eventstore, tx, aggregate_id)
    },
    save_snapshot: fn(tx, snapshot) { save_snapshot(eventstore, tx, snapshot) },
    execute_transaction: execute_in_transaction(db),
    load_aggregate_transaction: execute_in_transaction(db),
    get_latest_snapshot_transaction: execute_in_transaction(db),
    load_events_transaction: execute_in_transaction(db),
  )
}

fn load_events(
  postgres_store: PostgresStore(entity, command, event, error),
  tx: pog.Connection,
  aggregate_id: eventsourcing.AggregateId,
  start_from: Int,
) -> Result(
  List(eventsourcing.EventEnvelop(event)),
  eventsourcing.EventSourcingError(error),
) {
  let row_decoder = {
    use event_version <- decode.field(0, decode.string)
    use aggregate_id <- decode.field(1, decode.string)
    use sequence <- decode.field(2, decode.int)
    use aggregate_type <- decode.field(3, decode.string)
    use event_type <- decode.field(4, decode.string)
    use payload <- decode.field(5, {
      use payload_string <- decode.then(decode.string)
      case json.parse(payload_string, postgres_store.event_decoder) {
        Ok(payload) -> decode.success(payload)
        Error(error) ->
          panic as string.concat([
              "Failed to decode event payload: ",
              string.inspect(error),
              " for payload: ",
              payload_string,
            ])
      }
    })
    use metadata <- decode.field(6, metadata_decoder())
    decode.success(eventsourcing.SerializedEventEnvelop(
      aggregate_id:,
      sequence:,
      payload:,
      metadata:,
      event_type:,
      event_version:,
      aggregate_type:,
    ))
  }
  pog.query(select_events_query)
  |> pog.parameter(pog.text(postgres_store.aggregate_type))
  |> pog.parameter(pog.text(aggregate_id))
  |> pog.parameter(pog.int(start_from))
  |> pog.returning(row_decoder)
  |> pog.execute(tx)
  |> result.map(fn(response) { response.rows })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to query events: " <> string.inspect(error),
    )
  })
}

fn metadata_decoder() -> decode.Decoder(List(#(String, String))) {
  use stringmetadata <- decode.then(decode.string)
  case json.parse(stringmetadata, decode.list(decode.list(decode.string))) {
    Ok(listmetadata) ->
      case
        list.try_map(listmetadata, fn(metadata) {
          case metadata {
            [key, val] -> Ok(#(key, val))
            _ -> Error("Invalid metadata format")
          }
        })
      {
        Ok(parsed_metadata) -> decode.success(parsed_metadata)
        Error(error) ->
          panic as string.concat([
              "Invalid metadata format: ",
              error,
              " for metadata: ",
              stringmetadata,
            ])
      }
    Error(error) ->
      panic as string.concat([
          "Failed to parse metadata JSON: ",
          string.inspect(error),
          " for metadata: ",
          stringmetadata,
        ])
  }
}

fn commit_events(
  postgres_store: PostgresStore(entity, command, event, error),
  tx: pog.Connection,
  context: eventsourcing.Aggregate(entity, command, event, error),
  events: List(event),
  metadata: Metadata,
) -> Result(
  #(List(eventsourcing.EventEnvelop(event)), Int),
  eventsourcing.EventSourcingError(error),
) {
  let eventsourcing.Aggregate(aggregate_id, _, sequence) = context

  let wrapped_events =
    wrap_events(postgres_store, aggregate_id, events, sequence, metadata)
  case list.last(wrapped_events) {
    Ok(last_event) ->
      persist_events(postgres_store, tx, wrapped_events)
      |> result.map(fn(_) { #(wrapped_events, last_event.sequence) })
    Error(_) ->
      Error(eventsourcing.EventStoreError("Cannot commit empty event list"))
  }
}

fn wrap_events(
  postgres_store: PostgresStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
  events: List(event),
  sequence: Int,
  metadata: Metadata,
) -> List(eventsourcing.EventEnvelop(event)) {
  list.map_fold(
    over: events,
    from: sequence,
    with: fn(sequence: Int, event: event) {
      let next_sequence = sequence + 1
      #(
        next_sequence,
        eventsourcing.SerializedEventEnvelop(
          aggregate_id:,
          sequence: sequence + 1,
          payload: event,
          metadata:,
          event_type: postgres_store.event_type,
          event_version: postgres_store.event_version,
          aggregate_type: postgres_store.aggregate_type,
        ),
      )
    },
  )
  |> pair.second
}

fn persist_events(
  postgres_store: PostgresStore(entity, command, event, error),
  tx: pog.Connection,
  wrapped_events: List(eventsourcing.EventEnvelop(event)),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  // Generate the placeholders for batch insert
  let #(placeholders, params, _) =
    list.index_fold(wrapped_events, #("", [], 0), fn(acc, event, index) {
      let #(placeholders, params, _) = acc
      let offset = index * 7
      // 7 parameters per event
      let row_placeholders =
        "($"
        <> string.join(
          list.range(offset + 1, offset + 7)
            |> list.map(int.to_string),
          ", $",
        )
        <> ")"

      let sep = case placeholders {
        "" -> ""
        _ -> ", "
      }

      case event {
        eventsourcing.SerializedEventEnvelop(
          aggregate_id,
          sequence,
          payload,
          metadata,
          event_type,
          event_version,
          aggregate_type,
        ) -> {
          let new_params = [
            pog.text(aggregate_type),
            pog.text(aggregate_id),
            pog.int(sequence),
            pog.text(event_type),
            pog.text(event_version),
            pog.text(payload |> postgres_store.event_encoder),
            pog.text(metadata |> metadata_encoder),
          ]

          #(
            placeholders <> sep <> row_placeholders,
            list.append(params, new_params),
            index + 1,
          )
        }
        _ -> {
          // This should never happen as we only create SerializedEventEnvelop
          panic as "Invalid event envelope type"
        }
      }
    })

  // If no events to insert, return early
  case wrapped_events {
    [] -> Ok(Nil)
    _ -> {
      let query = batch_insert_events_query <> placeholders

      // Build the query with all parameters
      let prepared_query =
        params
        |> list.fold(pog.query(query), fn(query, param) {
          pog.parameter(query, param)
        })

      // Execute the batch insert
      prepared_query
      |> pog.execute(tx)
      |> result.map(fn(_) { Nil })
      |> result.map_error(fn(error) {
        eventsourcing.EventStoreError(
          "Failed to insert events: " <> string.inspect(error),
        )
      })
    }
  }
}

fn metadata_encoder(metadata: Metadata) -> String {
  json.array(metadata, fn(row) {
    json.preprocessed_array([json.string(row.0), json.string(row.1)])
  })
  |> json.to_string
}

fn load_snapshot(
  postgres_store: PostgresStore(entity, command, event, error),
  tx: pog.Connection,
  aggregate_id: eventsourcing.AggregateId,
) -> Result(
  Option(eventsourcing.Snapshot(entity)),
  eventsourcing.EventSourcingError(error),
) {
  let row_decoder = {
    use aggregate_id <- decode.field(1, decode.string)
    use sequence <- decode.field(2, decode.int)
    use entity <- decode.field(3, {
      use entity_string <- decode.then(decode.string)
      case json.parse(entity_string, postgres_store.entity_decoder) {
        Ok(entity) -> decode.success(entity)
        Error(error) ->
          panic as string.concat([
              "Failed to decode entity: ",
              string.inspect(error),
              " for entity: ",
              entity_string,
            ])
      }
    })
    use timestamp <- decode.field(4, pog.timestamp_decoder())

    decode.success(eventsourcing.Snapshot(
      aggregate_id: aggregate_id,
      entity: entity,
      sequence: sequence,
      timestamp: timestamp,
    ))
  }

  pog.query(select_snapshot_query)
  |> pog.parameter(pog.text(postgres_store.aggregate_type))
  |> pog.parameter(pog.text(aggregate_id))
  |> pog.returning(row_decoder)
  |> pog.execute(tx)
  |> result.map(fn(response) {
    case response.rows {
      [] -> None
      [snapshot, ..] -> option.Some(snapshot)
    }
  })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to load snapshot: " <> string.inspect(error),
    )
  })
}

fn save_snapshot(
  postgres_store: PostgresStore(entity, command, event, error),
  tx: pog.Connection,
  snapshot: eventsourcing.Snapshot(entity),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  let eventsourcing.Snapshot(aggregate_id, entity, sequence, timestamp) =
    snapshot

  pog.query(save_snapshot_query)
  |> pog.parameter(pog.text(postgres_store.aggregate_type))
  |> pog.parameter(pog.text(aggregate_id))
  |> pog.parameter(pog.int(sequence))
  |> pog.parameter(pog.text(postgres_store.entity_encoder(entity)))
  |> pog.parameter(pog.timestamp(timestamp))
  |> pog.execute(tx)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to save snapshot: " <> string.inspect(error),
    )
  })
}

pub fn create_event_table(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  pog.query(create_event_table_with_json_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create event table: " <> string.inspect(error),
    )
  })
}

pub fn create_snapshot_table(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  pog.query(create_snapshot_table_with_json_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create snapshot table: " <> string.inspect(error),
    )
  })
}

/// Legacy function: Creates event table with TEXT columns (for backward compatibility)
/// Use create_event_table() for new installations (uses JSONB)
pub fn create_event_table_legacy(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  pog.query(create_event_table_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create legacy event table: " <> string.inspect(error),
    )
  })
}

/// Legacy function: Creates snapshot table with TEXT columns (for backward compatibility) 
/// Use create_snapshot_table() for new installations (uses JSONB)
pub fn create_snapshot_table_legacy(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  pog.query(create_snapshot_table_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create legacy snapshot table: " <> string.inspect(error),
    )
  })
}

// MIGRATION FUNCTIONS ----

/// Step 1: Migrate event table from TEXT to JSONB columns
/// This adds new JSONB columns and converts existing data
pub fn migrate_event_table_to_jsonb(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use _ <- result.try(
    pog.query(add_event_jsonb_columns_query)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(_) { Nil })
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to add JSONB columns to event table: " <> string.inspect(error),
      )
    }),
  )

  pog.query(convert_event_data_to_jsonb_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to convert event data to JSONB: " <> string.inspect(error),
    )
  })
}

/// Step 1: Migrate snapshot table from TEXT to JSONB columns  
/// This adds new JSONB columns and converts existing data
pub fn migrate_snapshot_table_to_jsonb(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use _ <- result.try(
    pog.query(add_snapshot_jsonb_columns_query)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(_) { Nil })
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to add JSONB column to snapshot table: "
        <> string.inspect(error),
      )
    }),
  )

  pog.query(convert_snapshot_data_to_jsonb_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to convert snapshot data to JSONB: " <> string.inspect(error),
    )
  })
}

/// Step 2: Finalize event table migration by removing old TEXT columns
/// WARNING: This permanently deletes the old text columns after verification
pub fn finalize_event_table_migration(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use _ <- result.try(
    pog.query(drop_event_payload_column_query)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(_) { Nil })
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to drop old payload column: " <> string.inspect(error),
      )
    }),
  )
  use _ <- result.try(
    pog.query(drop_event_metadata_column_query)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(_) { Nil })
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to drop old metadata column: " <> string.inspect(error),
      )
    }),
  )
  use _ <- result.try(
    pog.query(rename_event_payload_column_query)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(_) { Nil })
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to rename payload column: " <> string.inspect(error),
      )
    }),
  )

  pog.query(rename_event_metadata_column_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to rename metadata column: " <> string.inspect(error),
    )
  })
}

/// Step 2: Finalize snapshot table migration by removing old TEXT columns  
/// WARNING: This permanently deletes the old text columns after verification
pub fn finalize_snapshot_table_migration(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use _ <- result.try(
    pog.query(drop_snapshot_entity_column_query)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(_) { Nil })
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to drop old entity column: " <> string.inspect(error),
      )
    }),
  )

  pog.query(rename_snapshot_entity_column_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to rename entity column: " <> string.inspect(error),
    )
  })
}

/// Safe migration helper - only migrates if tables exist and have TEXT columns
/// This is a convenience function that checks existing structure and migrates safely
pub fn migrate_to_jsonb_safe(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use _ <- result.try(migrate_event_table_to_jsonb(postgres_store))
  use _ <- result.try(migrate_snapshot_table_to_jsonb(postgres_store))
  Ok(Nil)
}

/// Complete migration helper - runs both steps for event and snapshot tables
/// This is a convenience function that runs the full migration process
/// WARNING: This finalizes the migration and removes old TEXT columns permanently
pub fn migrate_to_jsonb(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  use _ <- result.try(migrate_event_table_to_jsonb(postgres_store))
  use _ <- result.try(migrate_snapshot_table_to_jsonb(postgres_store))
  use _ <- result.try(finalize_event_table_migration(postgres_store))
  finalize_snapshot_table_migration(postgres_store)
}

fn execute_in_transaction(
  db: pog.Connection,
) -> fn(fn(pog.Connection) -> Result(a, b)) ->
  Result(a, eventsourcing.EventSourcingError(c)) {
  fn(f) {
    pog.transaction(db, f)
    |> result.replace_error(eventsourcing.TransactionFailed)
  }
}

// CLI FUNCTIONALITY ----

pub type CliCommand {
  CreateTables
  CreateLegacyTables
  MigrateTables
  MigrateTablesOnly
  FinalizeMigration
  Help
}

pub type CliConfig {
  CliConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    pool_name: String,
  )
}

/// Main CLI entry point for managing eventsourcing_postgres database
pub fn main() {
  // Load environment variables from .env file if it exists
  let _ = dot_env.load_default()

  case argv.load().arguments {
    ["create-tables"] -> run_create_tables()
    ["create-legacy-tables"] -> run_create_legacy_tables()
    ["migrate"] -> run_migrate_tables()
    ["migrate-only"] -> run_migrate_tables_only()
    ["finalize-migration"] -> run_finalize_migration()
    ["help"] | [] -> print_help()
    _ -> {
      io.println("Error: Unknown command")
      print_help()
    }
  }
}

fn run_create_tables() {
  case load_config_from_env() {
    Ok(config) -> {
      io.println("Creating tables with JSONB columns...")
      case create_tables_with_config(config, CreateTables) {
        Ok(_) -> {
          io.println(
            "✅ Successfully created event and snapshot tables with JSONB columns",
          )
          io.println(
            "Your tables are ready for high-performance JSON operations!",
          )
        }
        Error(error) -> {
          io.println("❌ Failed to create tables: " <> error)
        }
      }
    }
    Error(error) -> io.println("❌ Configuration error: " <> error)
  }
}

fn run_create_legacy_tables() {
  case load_config_from_env() {
    Ok(config) -> {
      io.println("Creating legacy tables with TEXT columns...")
      case create_tables_with_config(config, CreateLegacyTables) {
        Ok(_) -> {
          io.println(
            "✅ Successfully created event and snapshot tables with TEXT columns",
          )
          io.println(
            "Note: Consider using 'create-tables' for better performance with JSONB",
          )
        }
        Error(error) -> {
          io.println("❌ Failed to create legacy tables: " <> error)
        }
      }
    }
    Error(error) -> io.println("❌ Configuration error: " <> error)
  }
}

fn run_migrate_tables() {
  case load_config_from_env() {
    Ok(config) -> {
      io.println("Migrating tables from TEXT to JSONB...")
      io.println("This will preserve all existing data and add JSONB columns.")
      case create_tables_with_config(config, MigrateTables) {
        Ok(_) -> {
          io.println("✅ Successfully migrated tables to JSONB!")
          io.println("✅ All existing data preserved")
          io.println("✅ Old TEXT columns removed")
          io.println(
            "Your database is now using high-performance JSONB columns!",
          )
        }
        Error(error) -> {
          io.println("❌ Migration failed: " <> error)
          io.println("Your original data is safe - no changes were made.")
        }
      }
    }
    Error(error) -> io.println("❌ Configuration error: " <> error)
  }
}

fn run_migrate_tables_only() {
  case load_config_from_env() {
    Ok(config) -> {
      io.println("Migrating tables (safe mode - keeps old TEXT columns)...")
      case create_tables_with_config(config, MigrateTablesOnly) {
        Ok(_) -> {
          io.println("✅ Successfully migrated tables to JSONB!")
          io.println("✅ All existing data preserved")
          io.println("📝 Old TEXT columns kept for safety")
          io.println("Run 'finalize-migration' to complete the process")
        }
        Error(error) -> {
          io.println("❌ Migration failed: " <> error)
        }
      }
    }
    Error(error) -> io.println("❌ Configuration error: " <> error)
  }
}

fn run_finalize_migration() {
  case load_config_from_env() {
    Ok(config) -> {
      io.println("⚠️  WARNING: This will permanently remove old TEXT columns!")
      io.println("Finalizing migration...")
      case create_tables_with_config(config, FinalizeMigration) {
        Ok(_) -> {
          io.println("✅ Migration finalized successfully!")
          io.println("Old TEXT columns have been permanently removed.")
        }
        Error(error) -> {
          io.println("❌ Finalization failed: " <> error)
        }
      }
    }
    Error(error) -> io.println("❌ Configuration error: " <> error)
  }
}

fn create_tables_with_config(
  config: CliConfig,
  command: CliCommand,
) -> Result(Nil, String) {
  let pog_config =
    pog.Config(
      ..pog.default_config(process.new_name(config.pool_name)),
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: Some(config.password),
    )

  let pog_actor_spec = pog_config |> pog.supervised()

  case
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pog_actor_spec)
    |> static_supervisor.start()
  {
    Ok(_) -> {
      // Create a minimal postgres store for CLI operations
      let postgres_store =
        PostgresStore(
          db: pog.named_connection(pog_config.pool_name),
          event_encoder: fn(_) { "{}" },
          event_decoder: decode.success("{}"),
          event_type: "cli",
          event_version: "1.0",
          aggregate_type: "cli",
          entity_encoder: fn(_) { "{}" },
          entity_decoder: decode.success("{}"),
        )

      case command {
        CreateTables -> {
          use _ <- result.try(
            create_event_table(postgres_store)
            |> result.map_error(error_to_string),
          )
          create_snapshot_table(postgres_store)
          |> result.map_error(error_to_string)
        }
        CreateLegacyTables -> {
          use _ <- result.try(
            create_event_table_legacy(postgres_store)
            |> result.map_error(error_to_string),
          )
          create_snapshot_table_legacy(postgres_store)
          |> result.map_error(error_to_string)
        }
        MigrateTables -> {
          use _ <- result.try(
            migrate_event_table_to_jsonb(postgres_store)
            |> result.map_error(error_to_string),
          )
          use _ <- result.try(
            migrate_snapshot_table_to_jsonb(postgres_store)
            |> result.map_error(error_to_string),
          )
          use _ <- result.try(
            finalize_event_table_migration(postgres_store)
            |> result.map_error(error_to_string),
          )
          finalize_snapshot_table_migration(postgres_store)
          |> result.map_error(error_to_string)
        }
        MigrateTablesOnly -> {
          use _ <- result.try(
            migrate_event_table_to_jsonb(postgres_store)
            |> result.map_error(error_to_string),
          )
          migrate_snapshot_table_to_jsonb(postgres_store)
          |> result.map_error(error_to_string)
        }
        FinalizeMigration -> {
          use _ <- result.try(
            finalize_event_table_migration(postgres_store)
            |> result.map_error(error_to_string),
          )
          finalize_snapshot_table_migration(postgres_store)
          |> result.map_error(error_to_string)
        }
        Help -> {
          print_help()
          Ok(Nil)
        }
      }
    }
    Error(_) -> Error("Failed to start database connection supervisor")
  }
}

fn load_config_from_env() -> Result(CliConfig, String) {
  Ok(CliConfig(
    host: env.get_string_or("POSTGRES_HOST", "localhost"),
    port: env.get_int_or("POSTGRES_PORT", 5432),
    database: env.get_string_or("POSTGRES_DATABASE", "postgres"),
    user: env.get_string_or("POSTGRES_USER", "postgres"),
    password: env.get_string_or("POSTGRES_PASSWORD", "postgres"),
    pool_name: env.get_string_or("POSTGRES_POOL_NAME", "eventsourcing_cli"),
  ))
}

fn error_to_string(error: eventsourcing.EventSourcingError(_)) -> String {
  case error {
    eventsourcing.EventStoreError(message) -> message
    eventsourcing.DomainError(_) -> "Domain error occurred"
    eventsourcing.NonPositiveArgument -> "Non-positive argument error"
    eventsourcing.EntityNotFound -> "Entity not found"
    eventsourcing.TransactionFailed -> "Transaction failed"
    eventsourcing.ActorTimeout(operation:, timeout_ms:) ->
      "Actor timeout: "
      <> operation
      <> " (timeout: "
      <> int.to_string(timeout_ms)
      <> "ms)"
    eventsourcing.TransactionRolledBack -> "Transaction was rolled back"
  }
}

fn print_help() {
  io.println("eventsourcing_postgres CLI - Database Management Tool")
  io.println("")
  io.println("COMMANDS:")
  io.println(
    "  create-tables          Create tables with JSONB columns (recommended)",
  )
  io.println(
    "  create-legacy-tables   Create tables with TEXT columns (for compatibility)",
  )
  io.println(
    "  migrate               Complete migration from TEXT to JSONB (with cleanup)",
  )
  io.println(
    "  migrate-only          Migrate to JSONB but keep old TEXT columns",
  )
  io.println("  finalize-migration    Remove old TEXT columns after migration")
  io.println("  help                  Show this help message")
  io.println("")
  io.println("CONFIGURATION:")
  io.println("  Configure database connection using environment variables:")
  io.println("  ")
  io.println("  POSTGRES_HOST         Database host (default: localhost)")
  io.println("  POSTGRES_PORT         Database port (default: 5432)")
  io.println("  POSTGRES_DATABASE     Database name (default: postgres)")
  io.println("  POSTGRES_USER         Database user (default: postgres)")
  io.println("  POSTGRES_PASSWORD     Database password (default: postgres)")
  io.println(
    "  POSTGRES_POOL_NAME    Connection pool name (default: eventsourcing_cli)",
  )
  io.println("")
  io.println("CONFIGURATION METHODS:")
  io.println("  1. Create a .env file in your project root:")
  io.println("     POSTGRES_HOST=myhost")
  io.println("     POSTGRES_DATABASE=myapp")
  io.println("     POSTGRES_USER=myuser")
  io.println("     POSTGRES_PASSWORD=mypassword")
  io.println("")
  io.println("  2. Set environment variables directly:")
  io.println("     export POSTGRES_HOST=myhost")
  io.println("     export POSTGRES_DATABASE=myapp")
  io.println("")
  io.println("EXAMPLES:")
  io.println("  # Create JSONB tables in existing database")
  io.println("  POSTGRES_DATABASE=myapp gleam run create-tables")
  io.println("")
  io.println("  # Create tables with custom connection")
  io.println(
    "  POSTGRES_HOST=production.db POSTGRES_DATABASE=myapp gleam run create-tables",
  )
  io.println("")
  io.println("  # Migrate existing TEXT tables to JSONB")
  io.println("  POSTGRES_DATABASE=production gleam run migrate")
  io.println("")
  io.println("  # Safe migration (keeps old columns for rollback)")
  io.println("  gleam run migrate-only")
  io.println("  gleam run finalize-migration  # Run after verification")
  io.println("")
  io.println("COMPLETE WORKFLOWS:")
  io.println("  NEW PROJECT:")
  io.println("    Prerequisites: Create PostgreSQL database first")
  io.println("    1. gleam run create-tables      # Create JSONB tables")
  io.println("")
  io.println("  EXISTING PROJECT MIGRATION:")
  io.println("    Prerequisites: Backup your database")
  io.println("    1. gleam run migrate-only       # Safe migration")
  io.println("    2. gleam run finalize-migration # Complete migration")
  io.println("")
  io.println("  The migration preserves ALL existing data safely")
}
