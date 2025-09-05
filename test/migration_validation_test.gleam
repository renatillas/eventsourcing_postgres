import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{Some}
import gleam/otp/static_supervisor
import gleeunit
import pog

pub fn main() {
  gleeunit.main()
}

pub fn test_basic_migration_preserves_test() {
  let postgres_store = setup_test_store("basic_migration")

  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)

  let assert Ok(_) =
    eventsourcing_postgres.create_event_table_legacy(postgres_store.eventstore)
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table_legacy(
      postgres_store.eventstore,
    )

  clean_tables(postgres_store.eventstore.db)

  let event_sourcing = create_event_sourcing(postgres_store)

  eventsourcing.execute(
    event_sourcing,
    "migration-test-1",
    example_bank_account.OpenAccount("migration-test-1"),
  )

  eventsourcing.execute_with_metadata(
    event_sourcing,
    "migration-test-1",
    example_bank_account.DepositMoney(100.0),
    [#("test", "metadata")],
  )

  process.sleep(30)

  let events_before = count_events(postgres_store.eventstore.db)
  assert events_before == 2

  let assert Ok(_) =
    eventsourcing_postgres.migrate_event_table_to_jsonb(
      postgres_store.eventstore,
    )
  let assert Ok(_) =
    eventsourcing_postgres.migrate_snapshot_table_to_jsonb(
      postgres_store.eventstore,
    )

  let events_after = count_events(postgres_store.eventstore.db)
  assert events_after == 2

  eventsourcing.execute(
    event_sourcing,
    "migration-test-1",
    example_bank_account.WithDrawMoney(25.0),
  )

  process.sleep(20)

  let events_final = count_events(postgres_store.eventstore.db)
  assert events_final == 3

  clean_tables(postgres_store.eventstore.db)
}

pub fn test_idempotent_migration_test() {
  let postgres_store = setup_test_store("idempotent_migration")

  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)

  let assert Ok(_) =
    eventsourcing_postgres.create_event_table_legacy(postgres_store.eventstore)
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table_legacy(
      postgres_store.eventstore,
    )

  clean_tables(postgres_store.eventstore.db)

  let event_sourcing = create_event_sourcing(postgres_store)

  eventsourcing.execute(
    event_sourcing,
    "idempotent-test",
    example_bank_account.OpenAccount("idempotent-test"),
  )

  process.sleep(20)

  let events_initial = count_events(postgres_store.eventstore.db)
  assert events_initial == 1

  let assert Ok(_) =
    eventsourcing_postgres.migrate_event_table_to_jsonb(
      postgres_store.eventstore,
    )
  let assert Ok(_) =
    eventsourcing_postgres.migrate_snapshot_table_to_jsonb(
      postgres_store.eventstore,
    )

  let events_after_first = count_events(postgres_store.eventstore.db)
  assert events_after_first == 1

  let assert Ok(_) =
    eventsourcing_postgres.migrate_event_table_to_jsonb(
      postgres_store.eventstore,
    )
  let assert Ok(_) =
    eventsourcing_postgres.migrate_snapshot_table_to_jsonb(
      postgres_store.eventstore,
    )

  let events_after_second = count_events(postgres_store.eventstore.db)
  assert events_after_second == 1

  eventsourcing.execute(
    event_sourcing,
    "idempotent-test",
    example_bank_account.DepositMoney(50.0),
  )

  process.sleep(20)

  let events_final = count_events(postgres_store.eventstore.db)
  assert events_final == 2

  clean_tables(postgres_store.eventstore.db)
}

// Test that compares functionality between legacy TEXT and new JSONB table structures
// Uses separate databases to avoid primary key conflicts
pub fn test_new_table_vs_legacy_test() {
  let postgres_store_legacy = setup_test_store("legacy_comparison")

  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store_legacy.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store_legacy.eventstore.db)

  let assert Ok(_) =
    eventsourcing_postgres.create_event_table_legacy(
      postgres_store_legacy.eventstore,
    )
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table_legacy(
      postgres_store_legacy.eventstore,
    )

  clean_tables(postgres_store_legacy.eventstore.db)

  let event_sourcing_legacy = create_event_sourcing(postgres_store_legacy)

  let postgres_store_new = setup_test_store("new_comparison")

  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store_new.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store_new.eventstore.db)

  let assert Ok(_) =
    eventsourcing_postgres.create_event_table(postgres_store_new.eventstore)
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table(postgres_store_new.eventstore)

  clean_tables(postgres_store_new.eventstore.db)

  let event_sourcing_new = create_event_sourcing(postgres_store_new)

  // Create accounts with unique IDs to prevent primary key conflicts
  eventsourcing.execute(
    event_sourcing_legacy,
    "comparison-test-legacy",
    example_bank_account.OpenAccount("comparison-test-legacy"),
  )

  eventsourcing.execute(
    event_sourcing_new,
    "comparison-test-new",
    example_bank_account.OpenAccount("comparison-test-new"),
  )

  eventsourcing.execute(
    event_sourcing_legacy,
    "comparison-test-legacy",
    example_bank_account.DepositMoney(100.0),
  )
  eventsourcing.execute(
    event_sourcing_new,
    "comparison-test-new",
    example_bank_account.DepositMoney(100.0),
  )

  process.sleep(30)

  let events_legacy = count_events(postgres_store_legacy.eventstore.db)
  let events_new = count_events(postgres_store_new.eventstore.db)

  assert events_legacy == 2
  assert events_new == 2

  clean_tables(postgres_store_legacy.eventstore.db)
  clean_tables(postgres_store_new.eventstore.db)
}

fn setup_test_store(test_name: String) {
  let database_name = "eventsourcing_postgres_test" <> test_name
  let pog_config =
    pog.Config(
      ..pog.default_config(process.new_name(
        "migration_validation_" <> test_name,
      )),
      password: option.Some("postgres"),
      pool_size: 1,
    )

  let assert Ok(pog_started) = pog_config |> pog.start()
  process.sleep(100)
  create_database(pog_started.data, database_name)
  process.sleep(100)

  let pog_config =
    pog.Config(
      ..pog.default_config(process.new_name(
        "migration_validation_" <> test_name,
      )),
      password: option.Some("postgres"),
      database: database_name,
      pool_size: 1,
    )

  let assert Ok(_) = pog_config |> pog.start()

  eventsourcing_postgres.new(
    pgo_config: pog_config,
    event_encoder: example_bank_account.event_encoder,
    event_decoder: example_bank_account.event_decoder(),
    event_type: example_bank_account.bank_account_event_type,
    event_version: "1.0",
    aggregate_type: example_bank_account.bank_account_type,
    entity_encoder: example_bank_account.entity_encoder,
    entity_decoder: example_bank_account.entity_decoder(),
  )
}

fn create_event_sourcing(postgres_store) {
  let query = #(process.new_name("validation_query"), fn(_, _) { Nil })

  let assert Ok(frequency) = eventsourcing.frequency(1)
  let snapshot_config = eventsourcing.SnapshotConfig(frequency)

  let eventsourcing_name = process.new_name("validation_event_sourcing")
  let assert Ok(spec) =
    eventsourcing.supervised(
      name: eventsourcing_name,
      eventstore: postgres_store,
      handle: example_bank_account.handle,
      queries: [query],
      apply: example_bank_account.apply,
      empty_state: example_bank_account.BankAccount(opened: False, balance: 0.0),
      snapshot_config: Some(snapshot_config),
    )

  let assert Ok(_supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(spec)
    |> static_supervisor.start()

  process.named_subject(eventsourcing_name)
}

fn count_events(connection) {
  let row_decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(count)
  }

  let assert Ok(response) =
    pog.query("SELECT COUNT(*) FROM event")
    |> pog.returning(row_decoder)
    |> pog.execute(connection)

  case response.rows {
    [count] -> count
    _ -> 0
  }
}

fn clean_tables(connection) {
  let _ = pog.query("DELETE FROM event;") |> pog.execute(connection)
  let _ = pog.query("DELETE FROM snapshot;") |> pog.execute(connection)
  Nil
}

fn create_database(connection, db_name) {
  let query = "CREATE DATABASE " <> db_name <> ";"
  let _ = pog.query(query) |> pog.execute(connection)
  Nil
}
