import birdie
import eventsourcing
import eventsourcing_postgres
import example_bank_account
import gleam/option
import gleeunit
import gleeunit/should
import pog
import pprint

pub fn main() {
  gleeunit.main()
}

pub fn postgres_store_test() {
  let postgres_store = postgres_store()
  let query = fn(_, _) { Nil }

  eventsourcing_postgres.create_event_table(postgres_store.eventstore)
  |> should.be_ok

  let event_sourcing =
    eventsourcing.new(
      postgres_store,
      [query],
      example_bank_account.handle,
      example_bank_account.apply,
      example_bank_account.BankAccount(opened: False, balance: 0.0),
    )

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
  |> pprint.format
  |> birdie.snap(title: "postgres store")
}

fn postgres_store() {
  eventsourcing_postgres.new(
    pgo_config: pog.Config(
      ..pog.default_config(),
      password: option.Some("postgres"),
    ),
    event_encoder: example_bank_account.event_encoder,
    event_decoder: example_bank_account.event_decoder,
    event_type: example_bank_account.bank_account_event_type,
    event_version: "1.0",
    aggregate_type: example_bank_account.bank_account_type,
  )
}

pub fn postgres_store_load_events_test() {
  let postgres_store = postgres_store()
  let query = fn(_, _) { Nil }

  drop_event_table()
  |> should.be_ok

  eventsourcing_postgres.create_event_table(postgres_store.eventstore)
  |> should.be_ok

  let event_sourcing =
    eventsourcing.new(
      postgres_store,
      [query],
      example_bank_account.handle,
      example_bank_account.apply,
      example_bank_account.BankAccount(opened: False, balance: 0.0),
    )

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
  |> pprint.format
  |> birdie.snap(title: "postgres store load events")

  drop_event_table()
  |> should.be_ok
}

fn drop_event_table() {
  pog.query("DROP table event;")
  |> pog.execute(on: pog.connect(
    pog.Config(..pog.default_config(), password: option.Some("postgres")),
  ))
}
