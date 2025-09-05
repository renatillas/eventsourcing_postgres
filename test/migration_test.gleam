/// Test to demonstrate data preservation during migration
import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleam/otp/static_supervisor
import gleeunit
import pog

pub fn main() {
  gleeunit.main()
}

pub fn data_preservation_migration_test() {
  let postgres_store = setup_test_store()

  // Drop tables completely first to ensure clean state
  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)

  // Step 1: Create tables with TEXT columns and insert some data (simulating existing installation)
  let assert Ok(_) =
    eventsourcing_postgres.create_event_table_legacy(postgres_store.eventstore)
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table_legacy(
      postgres_store.eventstore,
    )

  // Clean tables first  
  let assert Ok(_) = clean_table("event", postgres_store.eventstore.db)
  let assert Ok(_) = clean_table("snapshot", postgres_store.eventstore.db)

  // Insert test data using the event sourcing system
  let event_sourcing = create_event_sourcing(postgres_store)

  // Create some events
  eventsourcing.execute(
    event_sourcing,
    "migration-test-account",
    example_bank_account.OpenAccount("migration-test-account"),
  )

  eventsourcing.execute(
    event_sourcing,
    "migration-test-account",
    example_bank_account.DepositMoney(100.0),
  )

  eventsourcing.execute(
    event_sourcing,
    "migration-test-account",
    example_bank_account.WithDrawMoney(25.0),
  )

  // Give time for async processing
  process.sleep(20)

  // Verify data exists in TEXT format (before migration)
  let events_before_migration =
    load_test_events(event_sourcing, "migration-test-account")
  assert list.length(events_before_migration) == 3

  // Step 2: Run migration to add JSONB columns and convert data
  let assert Ok(_) =
    eventsourcing_postgres.migrate_event_table_to_jsonb(
      postgres_store.eventstore,
    )
  let assert Ok(_) =
    eventsourcing_postgres.migrate_snapshot_table_to_jsonb(
      postgres_store.eventstore,
    )

  // Step 3: Verify data still exists and is readable after migration
  let events_after_migration =
    load_test_events(event_sourcing, "migration-test-account")
  assert list.length(events_after_migration) == 3

  // Step 4: Verify we can still add new events after migration
  eventsourcing.execute(
    event_sourcing,
    "migration-test-account",
    example_bank_account.DepositMoney(50.0),
  )

  process.sleep(20)

  let events_after_new_insert =
    load_test_events(event_sourcing, "migration-test-account")
  assert list.length(events_after_new_insert) == 4

  // Step 5: Test finalization (optional - be careful in real scenarios)
  // This demonstrates that finalization works but should be used carefully
  // let assert Ok(_) = eventsourcing_postgres.finalize_event_table_migration(postgres_store.eventstore)
  // let assert Ok(_) = eventsourcing_postgres.finalize_snapshot_table_migration(postgres_store.eventstore)

  // Clean up
  let assert Ok(_) = clean_table("event", postgres_store.eventstore.db)
  let assert Ok(_) = clean_table("snapshot", postgres_store.eventstore.db)
}

fn setup_test_store() {
  let pog_config =
    pog.Config(
      ..pog.default_config(process.new_name("migration_test")),
      password: option.Some("postgres"),
    )

  let pog_actor_spec = pog_config |> pog.supervised()

  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pog_actor_spec)
    |> static_supervisor.start()

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
  let query = #(process.new_name("query"), fn(_, _) { Nil })

  let assert Ok(frequency) = eventsourcing.frequency(1)
  let snapshot_config = eventsourcing.SnapshotConfig(frequency)

  let eventsourcing_name = process.new_name("migration_test_event_sourcing")
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

fn load_test_events(event_sourcing, aggregate_id) {
  let events_subject = eventsourcing.load_events(event_sourcing, aggregate_id)
  let assert Ok(events_result) = process.receive(events_subject, 1000)
  let assert Ok(events) = events_result
  events
}

fn clean_table(table_name, connection) {
  pog.query("DELETE FROM " <> table_name <> ";")
  |> pog.execute(connection)
}
