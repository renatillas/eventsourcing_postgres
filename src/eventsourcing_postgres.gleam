import eventsourcing
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None}
import gleam/pair
import gleam/result
import gleam/string
import pog
import pprint

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

const isolation_level_query = "LOCK TABLE event IN ACCESS EXCLUSIVE MODE;"

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
  let db = pog.connect(pgo_config)

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
      |> result.map_error(fn(error) {
        eventsourcing.EventStoreError(
          "Failed to load events: " <> string.inspect(error),
        )
      })
    },
    commit_events: fn(tx, aggregate_id, events, metadata) {
      commit_events(eventstore, tx, aggregate_id, events, metadata)
      |> result.map_error(fn(error) {
        eventsourcing.EventStoreError(
          "Failed to commit events: " <> string.inspect(error),
        )
      })
    },
    load_snapshot: fn(tx, aggregate_id) {
      load_snapshot(eventstore, tx, aggregate_id)
      |> result.map_error(fn(error) {
        eventsourcing.EventStoreError(
          "Failed to load snapshot: " <> string.inspect(error),
        )
      })
    },
    save_snapshot: fn(tx, snapshot) {
      save_snapshot(eventstore, tx, snapshot)
      |> result.map_error(fn(error) {
        eventsourcing.EventStoreError(
          "Failed to save snapshot: " <> string.inspect(error),
        )
      })
    },
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
  use _ <- result.try(
    pog.query(isolation_level_query)
    |> pog.execute(tx)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(
        "Failed to set isolation level: " <> pprint.format(error),
      )
    }),
  )

  let row_decoder = {
    use aggregate_id <- decode.field(1, decode.string)
    use sequence <- decode.field(2, decode.int)
    use payload <- decode.field(5, {
      use payload_string <- decode.then(decode.string)
      let assert Ok(payload) =
        json.parse(payload_string, postgres_store.event_decoder)
      decode.success(payload)
    })

    use metadata <- decode.field(6, metadata_decoder())
    use event_type <- decode.field(4, decode.string)
    use event_version <- decode.field(0, decode.string)
    use aggregate_type <- decode.field(3, decode.string)
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
      "Failed to query events: " <> pprint.format(error),
    )
  })
}

fn metadata_decoder() {
  use stringmetadata <- decode.then(decode.string)
  let assert Ok(listmetadata) =
    json.parse(stringmetadata, decode.list(decode.list(decode.string)))

  list.map(listmetadata, fn(metadata) {
    let assert [key, val] = metadata
    #(key, val)
  })
  |> decode.success
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
  let assert Ok(last_event) = list.last(wrapped_events)

  persist_events(postgres_store, tx, wrapped_events)
  |> result.map(fn(_) { #(wrapped_events, last_event.sequence) })
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

      let assert eventsourcing.SerializedEventEnvelop(
        aggregate_id,
        sequence,
        payload,
        metadata,
        event_type,
        event_version,
        aggregate_type,
      ) = event

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
          "Failed to insert events: " <> pprint.format(error),
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
      let assert Ok(entity) =
        json.parse(entity_string, postgres_store.entity_decoder)
      decode.success(entity)
    })
    use timestamp <- decode.field(4, decode.int)

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
      "Failed to load snapshot: " <> pprint.format(error),
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
  |> pog.parameter(pog.int(timestamp))
  |> pog.execute(tx)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to save snapshot: " <> pprint.format(pprint.format(error)),
    )
  })
}

pub fn create_snapshot_table(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  pog.query(create_snapshot_table_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create snapshot table: " <> pprint.format(error),
    )
  })
}

pub fn create_event_table(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(Nil, eventsourcing.EventSourcingError(error)) {
  pog.query(create_event_table_query)
  |> pog.execute(postgres_store.db)
  |> result.map(fn(_) { Nil })
  |> result.map_error(fn(error) {
    eventsourcing.EventStoreError(
      "Failed to create snapshot table: " <> pprint.format(error),
    )
  })
}

fn execute_in_transaction(db) {
  fn(f) {
    let f = fn(db) {
      f(db) |> result.map_error(fn(error) { string.inspect(error) })
    }
    pog.transaction(db, f)
    |> result.map_error(fn(error) {
      eventsourcing.EventStoreError(string.inspect(error))
    })
  }
}
