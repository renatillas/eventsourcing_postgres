import gleam/dynamic/decode
import gleam/json

pub type BankAccount {
  BankAccount(opened: Bool, balance: Float)
}

pub const bank_account_type = "BankAccount"

pub type BankAccountCommand {
  OpenAccount(account_id: String)
  DepositMoney(amount: Float)
  WithDrawMoney(amount: Float)
}

pub type BankAccountEvent {
  AccountOpened(account_id: String)
  CustomerDepositedCash(amount: Float, balance: Float)
  CustomerWithdrewCash(amount: Float, balance: Float)
}

pub const bank_account_event_type = "BankAccountEvent"

pub fn handle(
  bank_account: BankAccount,
  command: BankAccountCommand,
) -> Result(List(BankAccountEvent), Nil) {
  case bank_account, command {
    BankAccount(False, _), OpenAccount(account_id) ->
      Ok([AccountOpened(account_id)])
    BankAccount(True, _), OpenAccount(_) -> Error(Nil)
    BankAccount(True, _), DepositMoney(amount) -> {
      let balance = bank_account.balance +. amount
      case amount >. 0.0 {
        True -> Ok([CustomerDepositedCash(amount:, balance:)])
        False -> Error(Nil)
      }
    }
    BankAccount(True, _), WithDrawMoney(amount) -> {
      let balance = bank_account.balance -. amount
      case amount >. 0.0 && balance >=. 0.0 {
        True -> Ok([CustomerWithdrewCash(amount:, balance:)])
        False -> Error(Nil)
      }
    }
    _, _ -> Error(Nil)
  }
}

pub fn apply(bank_account: BankAccount, event: BankAccountEvent) {
  case event {
    AccountOpened(_) -> BankAccount(..bank_account, opened: True)
    CustomerDepositedCash(_, balance) -> BankAccount(..bank_account, balance:)
    CustomerWithdrewCash(_, balance) -> BankAccount(..bank_account, balance:)
  }
}

pub fn event_encoder(event: BankAccountEvent) -> String {
  case event {
    AccountOpened(account_id) ->
      json.object([
        #("event-type", json.string("account-opened")),
        #("account-id", json.string(account_id)),
      ])
    CustomerDepositedCash(amount, balance) ->
      json.object([
        #("event-type", json.string("customer-deposited-cash")),
        #("amount", json.float(amount)),
        #("balance", json.float(balance)),
      ])
    CustomerWithdrewCash(amount, balance) ->
      json.object([
        #("event-type", json.string("customer-withdrew-cash")),
        #("amount", json.float(amount)),
        #("balance", json.float(balance)),
      ])
  }
  |> json.to_string
}

// TODO: This may not work
pub fn event_decoder() -> decode.Decoder(BankAccountEvent) {
  let account_opened_decoder = {
    use account_id <- decode.field("account-id", decode.string)
    decode.success(AccountOpened(account_id))
  }

  let customer_deposited_cash = {
    use amount <- decode.field("amount", decode.float)
    use balance <- decode.field("balance", decode.float)
    decode.success(CustomerDepositedCash(amount, balance))
  }

  let customer_withdrew_cash = {
    use amount <- decode.field("amount", decode.float)
    use balance <- decode.field("balance", decode.float)
    decode.success(CustomerWithdrewCash(amount, balance))
  }

  use tag <- decode.field("event-type", decode.string)
  case tag {
    "account-opened" -> account_opened_decoder
    "customer-deposited-cash" -> customer_deposited_cash
    _ -> customer_withdrew_cash
  }
}

pub fn entity_encoder(bank: BankAccount) -> String {
  json.object([
    #("opened", json.bool(bank.opened)),
    #("balance", json.float(bank.balance)),
  ])
  |> json.to_string
}

pub fn entity_decoder() -> decode.Decoder(BankAccount) {
  use opened <- decode.field("opened", decode.bool)
  use balance <- decode.field("balance", decode.float)
  decode.success(BankAccount(opened, balance))
}
