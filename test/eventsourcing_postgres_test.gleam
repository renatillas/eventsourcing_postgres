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
  let query = fn(_, _) { Nil }

  let assert Ok(_) =
    eventsourcing_postgres.create_event_table(postgres_store.eventstore)
  let assert Ok(_) =
    eventsourcing_postgres.create_snapshot_table(postgres_store.eventstore)

  let event_sourcing =
    eventsourcing.new(
      postgres_store,
      [query],
      example_bank_account.handle,
      example_bank_account.apply,
      example_bank_account.BankAccount(opened: False, balance: 0.0),
    )

  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  happy_path(event_sourcing)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)

  load_events(event_sourcing)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)

  snapshots_happy_path(event_sourcing)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  snapshot_edge_cases(event_sourcing)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  snapshot_concurrent_updates(event_sourcing)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)

  snapshot_error_cases(event_sourcing, postgres_store.eventstore)
  let assert Ok(_) = delete_from_db("event", postgres_store.eventstore.db)
  let assert Ok(_) = delete_from_db("snapshot", postgres_store.eventstore.db)
}

fn happy_path(event_sourcing) {
  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.DepositMoney(10.0),
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.WithDrawMoney(5.99),
    )
  assert value == Nil

  let assert Ok(_) =
    eventsourcing.load_aggregate(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
    )
}

fn load_events(event_sourcing) {
  let assert Ok(value) =
    eventsourcing.execute_with_metadata(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
      [#("meta", "data")],
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.execute_with_metadata(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.DepositMoney(10.0),
      [],
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.WithDrawMoney(5.99),
    )
  assert value == Nil

  let assert Ok(_) =
    eventsourcing.load_events(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
    )
}

fn snapshots_happy_path(event_sourcing) {
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.OpenAccount("92085b42-032c-4d7a-84de-a86d67123858"),
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.DepositMoney(10.0),
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.execute(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
      example_bank_account.WithDrawMoney(5.99),
    )
  assert value == Nil

  let assert Ok(value) =
    eventsourcing.get_latest_snapshot(
      event_sourcing,
      "92085b42-032c-4d7a-84de-a86d67123858",
    )
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
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  // Test Case 1: Non-existent aggregate
  assert eventsourcing.get_latest_snapshot(event_sourcing, "non-existent-id")
    == Ok(None)

  // Test Case 2: Create and update snapshot

  // Open account
  let assert Ok(_) =
    eventsourcing.execute(
      event_sourcing,
      "snapshot-test-id",
      example_bank_account.OpenAccount("snapshot-test-id"),
    )

  // First snapshot should exist
  let assert Ok(value) =
    eventsourcing.get_latest_snapshot(event_sourcing, "snapshot-test-id")
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    assert sequence == 1
  }

  // Test Case 3: Multiple updates in sequence
  let assert Ok(_) =
    eventsourcing.execute(
      event_sourcing,
      "snapshot-test-id",
      example_bank_account.DepositMoney(100.0),
    )

  let assert Ok(_) =
    eventsourcing.execute(
      event_sourcing,
      "snapshot-test-id",
      example_bank_account.WithDrawMoney(30.0),
    )

  // Verify final snapshot state
  let assert Ok(value) =
    eventsourcing.get_latest_snapshot(event_sourcing, "snapshot-test-id")
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 70.0) =
      entity
    assert sequence == 3
  }

  // Test Case 4: Verify snapshot with empty metadata
  let assert Ok(_) =
    eventsourcing.execute_with_metadata(
      event_sourcing,
      "snapshot-test-id",
      example_bank_account.DepositMoney(30.0),
      [],
    )

  // Test Case 5: Verify snapshot with metadata
  let assert Ok(_) =
    eventsourcing.execute_with_metadata(
      event_sourcing,
      "snapshot-test-id",
      example_bank_account.WithDrawMoney(20.0),
      [#("operation", "withdrawal"), #("reason", "test")],
    )

  // Final state verification
  let assert Ok(value) =
    eventsourcing.get_latest_snapshot(event_sourcing, "snapshot-test-id")
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, timestamp)) =
      snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 80.0) =
      entity
    assert sequence == 5
    assert timestamp != 0
  }
}

fn snapshot_concurrent_updates(event_sourcing) {
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  // Test concurrent updates on same account
  let account_id = "concurrent-test-id"

  // Initialize account
  let assert Ok(_) =
    eventsourcing.execute(
      event_sourcing,
      account_id,
      example_bank_account.OpenAccount(account_id),
    )

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
      })
    })
  taskle.try_await_all(list.append(deposit_tasks, withdraw_tasks), 1000)

  // Load events to verify they were all recorded
  let assert Ok(value) = eventsourcing.load_events(event_sourcing, account_id)
  assert value
    |> list.length
    == 201
  // Verify final state
  let assert Ok(value) =
    eventsourcing.get_latest_snapshot(event_sourcing, account_id)
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
  let event_sourcing =
    event_sourcing
    |> eventsourcing.with_snapshots(eventsourcing.SnapshotConfig(1))

  let assert Ok(_) =
    pog.execute(pog.query("DROP TABLE snapshot"), event_store.db)
  // Test Case 1: Attempt operations before table creation
  let assert Error(_) =
    eventsourcing.get_latest_snapshot(event_sourcing, "error-test-id")

  // Create tables and test error cases
  let assert Ok(_) = eventsourcing_postgres.create_event_table(event_store)
  let assert Ok(_) = eventsourcing_postgres.create_snapshot_table(event_store)

  let account_id = "error-test-id"

  // Test Case 2: Operations on unopened account
  let assert Error(_) =
    eventsourcing.execute(
      event_sourcing,
      account_id,
      example_bank_account.WithDrawMoney(100.0),
    )

  // Test Case 3: Invalid operation sequence
  let assert Ok(_) =
    eventsourcing.execute(
      event_sourcing,
      account_id,
      example_bank_account.OpenAccount(account_id),
    )

  // Attempt to withdraw more than balance
  let assert Error(_) =
    eventsourcing.execute(
      event_sourcing,
      account_id,
      example_bank_account.WithDrawMoney(100.0),
    )

  // Verify snapshot still reflects valid state
  let assert Ok(value) =
    eventsourcing.get_latest_snapshot(event_sourcing, account_id)
  value
  |> fn(snapshot) {
    let assert Some(eventsourcing.Snapshot(_, entity, sequence, _)) = snapshot
    let assert example_bank_account.BankAccount(opened: True, balance: 0.0) =
      entity
    assert sequence == 1
  }
}
