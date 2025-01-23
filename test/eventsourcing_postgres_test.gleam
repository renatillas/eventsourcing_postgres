import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/task
import gleam/string
import gleeunit
import gleeunit/should
import pog

pub fn main() {
  gleeunit.main()
}

fn postgres_store() {
  eventsourcing_postgres.new(
    pgo_config: pog.Config(
      ..pog.default_config(),
      password: option.Some("postgres"),
    ),
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
  let query = fn(_, _) { Nil }

  eventsourcing_postgres.create_event_table(postgres_store.eventstore)
  |> should.be_ok
  eventsourcing_postgres.create_snapshot_table(postgres_store.eventstore)
  |> should.be_ok

  let event_sourcing =
    eventsourcing.new(
      postgres_store,
      [query],
      example_bank_account.handle,
      example_bank_account.apply,
      example_bank_account.BankAccount(opened: False, balance: 0.0),
    )

  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()
  delete_from_db("snapshot", postgres_store.eventstore.db)
  |> should.be_ok()

  happy_path(event_sourcing)
  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()

  load_events(event_sourcing)
  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()

  snapshots_happy_path(event_sourcing)
  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()
  delete_from_db("snapshot", postgres_store.eventstore.db)
  |> should.be_ok()

  snapshot_edge_cases(event_sourcing)
  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()
  delete_from_db("snapshot", postgres_store.eventstore.db)
  |> should.be_ok()

  snapshot_concurrent_updates(event_sourcing)
  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()
  delete_from_db("snapshot", postgres_store.eventstore.db)
  |> should.be_ok()

  snapshot_error_cases(event_sourcing, postgres_store.eventstore)
  delete_from_db("event", postgres_store.eventstore.db)
  |> should.be_ok()
  delete_from_db("snapshot", postgres_store.eventstore.db)
  |> should.be_ok()
}

fn happy_path(event_sourcing) {
  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.load_aggregate(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
  )
  |> should.be_ok
}

fn load_events(event_sourcing) {
  eventsourcing.execute_with_metadata(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
    [#("meta", "data")],
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute_with_metadata(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
    [],
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.load_events(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
  )
  |> should.be_ok
}

fn snapshots_happy_path(event_sourcing) {
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.DepositMoney(10.0),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.execute(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
    example_bank_account.WithDrawMoney(5.99),
  )
  |> should.be_ok
  |> should.equal(Nil)

  eventsourcing.get_latest_snapshot(
    event_sourcing,
    "92085b42-032c-4d7a-84de-a86d67123858",
  )
  |> should.be_ok
  |> fn(
    snapshot: Option(eventsourcing.Snapshot(example_bank_account.BankAccount)),
  ) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    entity.balance |> should.equal(4.01)
    sequence |> should.equal(3)
  }
}

fn snapshot_edge_cases(event_sourcing) {
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  // Test Case 1: Non-existent aggregate
  eventsourcing.get_latest_snapshot(event_sourcing, "non-existent-id")
  |> should.equal(Ok(None))

  // Test Case 2: Create and update snapshot

  // Open account
  eventsourcing.execute(
    event_sourcing,
    "snapshot-test-id",
    example_bank_account.OpenAccount("snapshot-test-id"),
  )
  |> should.be_ok

  // First snapshot should exist
  eventsourcing.get_latest_snapshot(event_sourcing, "snapshot-test-id")
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    sequence |> should.equal(1)
  }

  // Test Case 3: Multiple updates in sequence
  eventsourcing.execute(
    event_sourcing,
    "snapshot-test-id",
    example_bank_account.DepositMoney(100.0),
  )
  |> should.be_ok

  eventsourcing.execute(
    event_sourcing,
    "snapshot-test-id",
    example_bank_account.WithDrawMoney(30.0),
  )
  |> should.be_ok

  // Verify final snapshot state
  eventsourcing.get_latest_snapshot(event_sourcing, "snapshot-test-id")
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 70.0) =
      entity
    sequence |> should.equal(3)
  }

  // Test Case 4: Verify snapshot with empty metadata
  eventsourcing.execute_with_metadata(
    event_sourcing,
    "snapshot-test-id",
    example_bank_account.DepositMoney(30.0),
    [],
  )
  |> should.be_ok

  // Test Case 5: Verify snapshot with metadata
  eventsourcing.execute_with_metadata(
    event_sourcing,
    "snapshot-test-id",
    example_bank_account.WithDrawMoney(20.0),
    [#("operation", "withdrawal"), #("reason", "test")],
  )
  |> should.be_ok

  // Final state verification
  eventsourcing.get_latest_snapshot(event_sourcing, "snapshot-test-id")
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, timestamp)) =
      snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 80.0) =
      entity
    sequence |> should.equal(5)
    timestamp |> should.not_equal(0)
  }
}

fn snapshot_concurrent_updates(event_sourcing) {
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  // Test concurrent updates on same account
  let account_id = "concurrent-test-id"

  // Initialize account
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.OpenAccount(account_id),
  )
  |> should.be_ok

  // Create 100 concurrent deposit tasks
  let deposit_tasks =
    list.range(1, 100)
    |> list.map(fn(i) {
      task.async(fn() {
        eventsourcing.execute_with_metadata(
          event_sourcing,
          account_id,
          example_bank_account.DepositMoney(1.0),
          [#("concurrent_operation", string.inspect(i))],
        )
      })
    })
  let withdraw_tasks =
    list.range(1, 100)
    |> list.map(fn(i) {
      task.async(fn() {
        eventsourcing.execute_with_metadata(
          event_sourcing,
          account_id,
          example_bank_account.WithDrawMoney(1.0),
          [#("concurrent_operation", string.inspect(i))],
        )
      })
    })
  task.try_await_all(list.append(deposit_tasks, withdraw_tasks), 10)

  // Load events to verify they were all recorded
  eventsourcing.load_events(event_sourcing, account_id)
  |> should.be_ok
  |> list.length
  |> should.equal(201)
  // Verify final state
  eventsourcing.get_latest_snapshot(event_sourcing, account_id)
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    sequence |> should.equal(201)
  }
}

fn snapshot_error_cases(
  event_sourcing,
  event_store: eventsourcing_postgres.PostgresStore(_, _, _, _),
) {
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  pog.query("DROP TABLE snapshot")
  |> pog.execute(event_store.db)
  |> should.be_ok()
  // Test Case 1: Attempt operations before table creation
  eventsourcing.get_latest_snapshot(event_sourcing, "error-test-id")
  |> should.be_error

  // Create tables and test error cases
  eventsourcing_postgres.create_event_table(event_store)
  |> should.be_ok
  eventsourcing_postgres.create_snapshot_table(event_store)
  |> should.be_ok

  let account_id = "error-test-id"

  // Test Case 2: Operations on unopened account
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.WithDrawMoney(100.0),
  )
  |> should.be_error

  // Test Case 3: Invalid operation sequence
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.OpenAccount(account_id),
  )
  |> should.be_ok

  // Attempt to withdraw more than balance
  eventsourcing.execute(
    event_sourcing,
    account_id,
    example_bank_account.WithDrawMoney(100.0),
  )
  |> should.be_error

  // Verify snapshot still reflects valid state
  eventsourcing.get_latest_snapshot(event_sourcing, account_id)
  |> should.be_ok
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    sequence |> should.equal(1)
  }
}
