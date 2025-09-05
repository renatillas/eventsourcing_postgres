import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/static_supervisor
import gleam/string
import gleeunit

import pog
import taskle

pub fn main() {
  gleeunit.main()
}

fn postgres_store() {
  let pog_config =
    pog.Config(
      ..pog.default_config(process.new_name("postgres_store_test")),
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

fn delete_from_db(table, connection) {
  pog.query("DELETE FROM " <> table <> ";")
  |> pog.execute(connection)
}

pub fn postgres_store_test() {
  let postgres_store = postgres_store()

  // Drop tables completely first to ensure clean state
  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)

  let assert Ok(_) =
    eventsourcing_postgres.create_event_table(postgres_store.eventstore)
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table(postgres_store.eventstore)

  // Clean initial state
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  let _ = happy_path_test(postgres_store)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)

  let _ = load_events_test(postgres_store)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)

  let _ = snapshots_happy_path_test(postgres_store)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  let _ = snapshot_edge_cases_test(postgres_store)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  let _ = snapshot_concurrent_updates_test(postgres_store)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  let _ = snapshot_error_cases_test(postgres_store)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)
}

fn create_event_sourcing(postgres_store) {
  let query = #(process.new_name("query"), fn(_, _) { Nil })

  let assert Ok(frequency) = eventsourcing.frequency(1)
  let snapshot_config = eventsourcing.SnapshotConfig(frequency)

  let eventsourcing_name = process.new_name("event_sourcing_actor")
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

  // Start the supervision tree
  let assert Ok(_supervisor) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(spec)
    |> static_supervisor.start()

  process.named_subject(eventsourcing_name)
}

fn happy_path_test(postgres_store) {
  let eventsourcing = create_event_sourcing(postgres_store)
  happy_path(eventsourcing)
}

fn load_events_test(postgres_store) {
  let event_sourcing = create_event_sourcing(postgres_store)
  load_events(event_sourcing)
}

fn snapshots_happy_path_test(postgres_store) {
  let event_sourcing = create_event_sourcing(postgres_store)
  snapshots_happy_path(event_sourcing)
}

fn snapshot_edge_cases_test(postgres_store) {
  let event_sourcing = create_event_sourcing(postgres_store)
  snapshot_edge_cases(event_sourcing)
}

fn snapshot_concurrent_updates_test(postgres_store) {
  let event_sourcing = create_event_sourcing(postgres_store)
  snapshot_concurrent_updates(event_sourcing)
}

fn snapshot_error_cases_test(postgres_store) {
  let event_sourcing = create_event_sourcing(postgres_store)
  snapshot_error_cases(event_sourcing, postgres_store.eventstore)
}

fn happy_path(event_sourcing) {
  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
  )

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
  )

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
  )

  // Give some time for async processing
  process.sleep(10)

  let aggregate_subject =
    eventsourcing.load_aggregate(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
    )
  let assert Ok(aggregate_result) = process.receive(aggregate_subject, 1000)
  let assert Ok(_) = aggregate_result
}

fn load_events(event_sourcing) {
  eventsourcing.execute_with_metadata(
    event_sourcing,
    "load-events-test-id",
    example_bank_account.OpenAccount("load-events-test-id"),
    [#("meta", "data")],
  )

  eventsourcing.execute_with_metadata(
    event_sourcing,
    "load-events-test-id",
    example_bank_account.DepositMoney(10.0),
    [],
  )

  eventsourcing.execute(
    event_sourcing,
    "load-events-test-id",
    example_bank_account.WithDrawMoney(5.99),
  )

  // Give some time for async processing
  process.sleep(10)

  let events_subject =
    eventsourcing.load_events(event_sourcing, "load-events-test-id")
  let assert Ok(events_result) = process.receive(events_subject, 1000)
  let assert Ok(_) = events_result
}

fn snapshots_happy_path(event_sourcing) {
  let account_id = "snapshots-happy-path-id"

  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.OpenAccount(account_id),
  )

  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.DepositMoney(10.0),
  )

  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.WithDrawMoney(5.99),
  )

  // Give some time for async processing
  process.sleep(10)

  let snapshot_subject =
    eventsourcing.latest_snapshot(event_sourcing, account_id)
  let assert Ok(snapshot_result) = process.receive(snapshot_subject, 1000)
  let assert Ok(value) = snapshot_result
  value
  |> fn(
    snapshot: Option(eventsourcing.Snapshot(example_bank_account.BankAccount)),
  ) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    assert entity.balance == 4.01
    assert sequence == 3
  }
}

fn snapshot_edge_cases(event_sourcing) {
  // Test Case 1: Non-existent aggregate
  let snapshot_subject =
    eventsourcing.latest_snapshot(event_sourcing, "non-existent-id")
  let assert Ok(snapshot_result) = process.receive(snapshot_subject, 1000)
  let assert Ok(snapshot_value) = snapshot_result
  assert snapshot_value == None

  // Test Case 2: Create and update snapshot
  let account_id = "snapshot-edge-cases-id"

  // Open account
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.OpenAccount(account_id),
  )

  // Give some time for async processing
  process.sleep(10)

  // First snapshot should exist
  let snapshot_subject =
    eventsourcing.latest_snapshot(event_sourcing, account_id)
  let assert Ok(snapshot_result) = process.receive(snapshot_subject, 1000)
  let assert Ok(value) = snapshot_result
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    assert sequence == 1
  }

  // Test Case 3: Multiple updates in sequence
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.DepositMoney(100.0),
  )

  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.WithDrawMoney(30.0),
  )

  // Give some time for async processing
  process.sleep(10)

  // Verify final snapshot state
  let snapshot_subject2 =
    eventsourcing.latest_snapshot(event_sourcing, account_id)
  let assert Ok(snapshot_result2) = process.receive(snapshot_subject2, 1000)
  let assert Ok(value) = snapshot_result2
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 70.0) =
      entity
    assert sequence == 3
  }

  // Test Case 4: Verify snapshot with empty metadata
  eventsourcing.execute_with_metadata(
    event_sourcing,
    account_id,
    example_bank_account.DepositMoney(30.0),
    [],
  )

  // Test Case 5: Verify snapshot with metadata
  eventsourcing.execute_with_metadata(
    event_sourcing,
    account_id,
    example_bank_account.WithDrawMoney(20.0),
    [#("operation", "withdrawal"), #("reason", "test")],
  )

  // Give some time for async processing
  process.sleep(10)

  // Final state verification
  let snapshot_subject3 =
    eventsourcing.latest_snapshot(event_sourcing, account_id)
  let assert Ok(snapshot_result3) = process.receive(snapshot_subject3, 1000)
  let assert Ok(value) = snapshot_result3
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _timestamp)) =
      snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 80.0) =
      entity
    assert sequence == 5
    // Note: timestamp comparison may need adjustment for v8.0.0
  }
}

fn snapshot_concurrent_updates(event_sourcing) {
  // Test concurrent updates on same account
  let account_id = "concurrent-updates-id"

  // Initialize account
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.OpenAccount(account_id),
  )

  // Give some time for async processing
  process.sleep(10)

  // Create 100 concurrent deposit tasks
  let deposit_tasks =
    list.range(1, 100)
    |> list.map(fn(i) {
      taskle.async(fn() {
        eventsourcing.execute_with_metadata(
          event_sourcing,
          account_id,
          example_bank_account.DepositMoney(1.0),
          [#("concurrent_operation", string.inspect(i))],
        )
        Ok(Nil)
      })
    })
  let withdraw_tasks =
    list.range(1, 100)
    |> list.map(fn(i) {
      taskle.async(fn() {
        eventsourcing.execute_with_metadata(
          event_sourcing,
          account_id,
          example_bank_account.WithDrawMoney(1.0),
          [#("concurrent_operation", string.inspect(i))],
        )
        Ok(Nil)
      })
    })
  let _ = taskle.try_await_all(list.append(deposit_tasks, withdraw_tasks), 1000)

  // Load events to verify they were all recorded
  let events_subject = eventsourcing.load_events(event_sourcing, account_id)
  let assert Ok(events_result) = process.receive(events_subject, 1000)
  let assert Ok(value) = events_result
  assert value
    |> list.length
    == 201
  // Verify final state
  let snapshot_subject =
    eventsourcing.latest_snapshot(event_sourcing, account_id)
  let assert Ok(snapshot_result) = process.receive(snapshot_subject, 1000)
  let assert Ok(value) = snapshot_result
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    assert sequence == 201
  }
}

fn snapshot_error_cases(
  event_sourcing,
  event_store: eventsourcing_postgres.PostgresStore(_, _, _, _),
) {
  // Test Case 1: Intentionally drop snapshot table to test error handling
  // This will generate expected "relation snapshot does not exist" errors in logs
  let assert Ok(_) =
    pog.execute(pog.query("DROP TABLE snapshot"), event_store.db)
  // Test Case 1: Attempt operations before table creation
  let snapshot_subject =
    eventsourcing.latest_snapshot(event_sourcing, "error-cases-before-table-id")
  let assert Ok(snapshot_result) = process.receive(snapshot_subject, 1000)
  let assert Error(_) = snapshot_result

  // Create tables and test error cases
  let assert Ok(_) = eventsourcing_postgres.create_event_table(event_store)
  let assert Ok(_) = eventsourcing_postgres.create_snapshot_table(event_store)

  let account_id = "error-cases-id"

  // Test Case 2: Valid operations only (avoid domain errors that crash actors)
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.OpenAccount(account_id),
  )

  // Give some time for async processing
  process.sleep(10)

  // Verify snapshot reflects valid state
  let snapshot_subject =
    eventsourcing.latest_snapshot(event_sourcing, account_id)
  let assert Ok(snapshot_result) = process.receive(snapshot_subject, 1000)
  let assert Ok(value) = snapshot_result
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    assert sequence == 1
  }
}
