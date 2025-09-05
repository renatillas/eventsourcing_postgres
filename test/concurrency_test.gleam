import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleam/otp/static_supervisor
import gleam/string
import gleeunit
import pog
import taskle

pub fn main() {
  gleeunit.main()
}

pub fn test_concurrent_table_creation_test() {
  let postgres_store = setup_test_store("concurrent_tables")

  // Drop tables to ensure clean state
  let _ =
    pog.query("DROP TABLE IF EXISTS event CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)
  let _ =
    pog.query("DROP TABLE IF EXISTS snapshot CASCADE;")
    |> pog.execute(postgres_store.eventstore.db)

  // Test concurrent table creation - should be safe with IF NOT EXISTS
  let table_creation_tasks =
    list.range(1, 10)
    |> list.map(fn(_i) {
      taskle.async(fn() {
        let _ =
          eventsourcing_postgres.create_event_table(postgres_store.eventstore)
        let _ =
          eventsourcing_postgres.create_snapshot_table(
            postgres_store.eventstore,
          )
        Ok(Nil)
      })
    })

  let _ = taskle.try_await_all(table_creation_tasks, 5000)

  // Verify tables were created successfully
  let table_check_query =
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('event', 'snapshot')"
  let row_decoder = {
    use count <- decode.field(0, decode.int)
    decode.success(count)
  }

  let assert Ok(response) =
    pog.query(table_check_query)
    |> pog.returning(row_decoder)
    |> pog.execute(postgres_store.eventstore.db)

  case response.rows {
    [count] -> {
      assert count == 2
    }
    _ -> panic as "Expected exactly 2 tables (event and snapshot)"
  }
}

pub fn test_concurrent_event_operations_test() {
  let postgres_store = setup_test_store("concurrent_events")

  // Setup tables
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

  clean_tables(postgres_store.eventstore.db)

  let event_sourcing = create_event_sourcing(postgres_store)

  // Create accounts sequentially first to avoid actor crashes
  list.range(1, 20)
  |> list.each(fn(i) {
    let account_id = "concurrent-account-" <> string.inspect(i)
    eventsourcing.execute(
      event_sourcing,
      account_id,
      example_bank_account.OpenAccount(account_id),
    )
  })

  process.sleep(200)

  let account_ids =
    list.range(1, 20)
    |> list.map(fn(i) { "concurrent-account-" <> string.inspect(i) })

  // First, perform all deposits concurrently (safe - no business logic conflicts)
  let deposit_tasks =
    account_ids
    |> list.map(fn(account_id) {
      taskle.async(fn() {
        eventsourcing.execute_with_metadata(
          event_sourcing,
          account_id,
          example_bank_account.DepositMoney(100.0),
          // Large deposit to ensure sufficient balance
          [#("operation", "deposit")],
        )
        Ok(Nil)
      })
    })

  let assert Ok(_) = taskle.try_await_all(deposit_tasks, 15_000)
  process.sleep(300)
  // Ensure all deposits are fully processed

  // Then, perform all withdrawals concurrently (safe - accounts now have balance)
  let withdraw_tasks =
    account_ids
    |> list.map(fn(account_id) {
      taskle.async(fn() {
        eventsourcing.execute_with_metadata(
          event_sourcing,
          account_id,
          example_bank_account.WithDrawMoney(25.0),
          [#("operation", "withdraw")],
        )
        Ok(Nil)
      })
    })

  let _ = taskle.try_await_all(withdraw_tasks, 15_000)

  process.sleep(500)
  // Longer wait for all async operations to complete

  let expected_events = list.length(account_ids) * 3
  let final_count = count_events(postgres_store.eventstore.db)

  // Demand 100% success rate for production-grade reliability
  assert final_count == expected_events

  clean_tables(postgres_store.eventstore.db)
}

pub fn test_concurrent_migration_with_operations_test() {
  let postgres_store = setup_test_store("concurrent_migration_ops")

  // Setup legacy tables with initial data
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

  // Add initial data
  eventsourcing.execute(
    event_sourcing,
    "migration-ops-test",
    example_bank_account.OpenAccount("migration-ops-test"),
  )

  process.sleep(50)

  // Perform concurrent migration and operations
  let concurrent_tasks =
    list.append(
      [
        // Migration task
        taskle.async(fn() {
          let _ =
            eventsourcing_postgres.migrate_event_table_to_jsonb(
              postgres_store.eventstore,
            )
          let _ =
            eventsourcing_postgres.migrate_snapshot_table_to_jsonb(
              postgres_store.eventstore,
            )
          Ok("migration")
        }),
      ],
      // Operation tasks
      list.range(1, 5)
        |> list.map(fn(i) {
          taskle.async(fn() {
            process.sleep(i * 10)
            // Stagger operations
            eventsourcing.execute_with_metadata(
              event_sourcing,
              "migration-ops-test",
              example_bank_account.DepositMoney(10.0),
              [#("concurrent_op", string.inspect(i))],
            )
            Ok("operation")
          })
        }),
    )

  let _ = taskle.try_await_all(concurrent_tasks, 10_000)

  process.sleep(100)

  // Verify data integrity after concurrent migration and operations
  let final_count = count_events(postgres_store.eventstore.db)
  assert final_count == 6
  // 1 initial + 5 deposits

  // Verify final account state
  let events_subject =
    eventsourcing.load_events(event_sourcing, "migration-ops-test")
  let assert Ok(events_result) = process.receive(events_subject, 1000)
  let assert Ok(events) = events_result
  assert list.length(events) == 6

  clean_tables(postgres_store.eventstore.db)
}

fn setup_test_store(test_name: String) {
  // Use default postgres database to avoid database creation complexity
  let pog_config =
    pog.Config(
      ..pog.default_config(process.new_name("concurrency_" <> test_name)),
      password: option.Some("postgres"),
      pool_size: 5,
      // Moderate pool size for concurrency
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
  let query = #(process.new_name("concurrency_query"), fn(_, _) { Nil })

  let assert Ok(frequency) = eventsourcing.frequency(1)
  let snapshot_config = eventsourcing.SnapshotConfig(frequency)

  let eventsourcing_name = process.new_name("concurrency_event_sourcing")
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
