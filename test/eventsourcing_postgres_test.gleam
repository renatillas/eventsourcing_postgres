import birdie
import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/dynamic
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/pgo
import gleeunit
import gleeunit/should
import pprint

pub fn main() {
  gleeunit.main()
}

pub fn postgres_store_test() {
  let postgres_event_store =
    eventsourcing_postgres.new(
      pgo_config: pgo.Config(
        ..pgo.default_config(),
        password: option.Some("postgres"),
      ),
      empty_entity: example_bank_account.BankAccount(
        opened: False,
        balance: 0.0,
      ),
      handle_command_function: example_bank_account.handle,
      apply_function: example_bank_account.apply,
      event_encoder: example_bank_account.event_encoder,
      event_decoder: example_bank_account.event_decoder,
      event_type: example_bank_account.bank_account_event_type,
      event_version: "1.0",
      aggregate_type: example_bank_account.bank_account_type,
    )
  let query = fn(
    aggregate_id: String,
    events: List(
      eventsourcing.EventEnvelop(example_bank_account.BankAccountEvent),
    ),
  ) {
    io.println_error(
      "Aggregate Bank Account with ID: "
      <> aggregate_id
      <> " commited "
      <> events |> list.length |> int.to_string
      <> " events.",
    )
  }
  drop_event_table()
  |> should.be_ok
  eventsourcing_postgres.create_event_table(postgres_event_store.eventstore)
  |> should.be_ok

  let event_sourcing = eventsourcing.new(postgres_event_store, [query])

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

  eventsourcing_postgres.load_aggregate_entity(
    postgres_event_store.eventstore,
    "92085b42-032c-4d7a-84de-a86d67123858",
  )
  |> pprint.format
  |> birdie.snap(title: "postgres store")
}

pub fn postgres_store_load_events_test() {
  let postgres_store =
    eventsourcing_postgres.new(
      pgo_config: pgo.Config(
        ..pgo.default_config(),
        password: option.Some("postgres"),
      ),
      empty_entity: example_bank_account.BankAccount(
        opened: False,
        balance: 0.0,
      ),
      handle_command_function: example_bank_account.handle,
      apply_function: example_bank_account.apply,
      event_encoder: example_bank_account.event_encoder,
      event_decoder: example_bank_account.event_decoder,
      event_type: example_bank_account.bank_account_event_type,
      event_version: "1.0",
      aggregate_type: example_bank_account.bank_account_type,
    )
  let query = fn(
    aggregate_id: String,
    events: List(
      eventsourcing.EventEnvelop(example_bank_account.BankAccountEvent),
    ),
  ) {
    io.println_error(
      "Aggregate Bank Account with ID: "
      <> aggregate_id
      <> " commited "
      <> events |> list.length |> int.to_string
      <> " events.",
    )
  }
  drop_event_table()
  |> should.be_ok
  eventsourcing_postgres.create_event_table(postgres_store.eventstore)
  |> should.be_ok

  let event_sourcing = eventsourcing.new(postgres_store, [query])

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

  eventsourcing_postgres.load_events(
    postgres_store.eventstore,
    "92085b42-032c-4d7a-84de-a86d67123858",
  )
  |> should.be_ok
  |> pprint.format
  |> birdie.snap(title: "postgres store load events")

  drop_event_table()
  |> should.be_ok
}

fn drop_event_table() {
  pgo.execute(
    query: "DROP table event;",
    on: pgo.connect(
      pgo.Config(..pgo.default_config(), password: option.Some("postgres")),
    ),
    expecting: dynamic.dynamic,
    with: [],
  )
}
