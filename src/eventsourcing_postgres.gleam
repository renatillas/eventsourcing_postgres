import eventsourcing
import gleam/dynamic/decode
import gleam/json
import gleam/list
import gleam/pair
import gleam/result
import pog

// CONSTANTS ----

const insert_event_query = "
  INSERT INTO event 
  (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
  VALUES 
  ($1, $2, $3, $4, $5, $6, $7)
  "

const select_events_query = "
  SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM event
  WHERE aggregate_type = $1 AND aggregate_id = $2
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

// TYPES ----

type Metadata =
  List(#(String, String))

pub opaque type PostgresStore(entity, command, event, error) {
  PostgresStore(
    db: pog.Connection,
    empty_aggregate: eventsourcing.Aggregate(entity, command, event, error),
    event_encoder: fn(event) -> String,
    event_decoder: fn(String) -> Result(event, List(decode.DecodeError)),
    event_type: String,
    event_version: String,
    aggregate_type: String,
  )
}

// CONSTRUCTORS ----

pub fn new(
  pgo_config pgo_config: pog.Config,
  empty_entity empty_entity: entity,
  handle_command_function handle: eventsourcing.Handle(
    entity,
    command,
    event,
    error,
  ),
  apply_function apply: eventsourcing.Apply(entity, event),
  event_encoder event_encoder: fn(event) -> String,
  event_decoder event_decoder: fn(String) ->
    Result(event, List(decode.DecodeError)),
  event_type event_type: String,
  event_version event_version: String,
  aggregate_type aggregate_type: String,
) -> eventsourcing.EventStore(
  PostgresStore(entity, command, event, error),
  entity,
  command,
  event,
  error,
) {
  let db = pog.connect(pgo_config)

  let eventstore =
    PostgresStore(
      db:,
      empty_aggregate: eventsourcing.Aggregate(empty_entity, handle, apply),
      event_encoder:,
      event_decoder:,
      event_type:,
      event_version:,
      aggregate_type:,
    )

  eventsourcing.EventStore(
    eventstore:,
    commit: commit,
    load_aggregate: load_aggregate,
    load_events: load_events,
  )
}

pub fn create_event_table(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(pog.Returned(Nil), pog.QueryError) {
  pog.query(create_event_table_query)
  |> pog.execute(postgres_store.db)
}

fn load_events(
  postgres_store: PostgresStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> List(eventsourcing.EventEnvelop(event)) {
  let row_decoder = {
    use aggregate_id <- decode.field(1, decode.string)
    use sequence <- decode.field(2, decode.int)
    use payload <- decode.field(5, {
      use payload <- decode.then(decode.string)
      // TODO:  remove this assert 
      let assert Ok(payload) = postgres_store.event_decoder(payload)
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
  let assert Ok(rows) =
    pog.query(select_events_query)
    |> pog.parameter(pog.text(postgres_store.aggregate_type))
    |> pog.parameter(pog.text(aggregate_id))
    |> pog.returning(row_decoder)
    |> pog.execute(postgres_store.db)
    |> result.map(fn(response) { response.rows })
  rows
}

fn metadata_decoder() {
  use stringmetadata <- decode.then(decode.string)
  // TODO: remove assert 
  let assert Ok(listmetadata) =
    json.parse(stringmetadata, decode.list(decode.list(decode.string)))

  list.map(listmetadata, fn(metadata) {
    // TODO: remove assert
    let assert [key, val] = metadata
    #(key, val)
  })
  |> decode.success
}

fn load_aggregate(
  postgres_store: PostgresStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> eventsourcing.AggregateContext(entity, command, event, error) {
  let commited_events = load_events(postgres_store, aggregate_id)

  let #(aggregate, sequence) =
    list.fold(
      over: commited_events,
      from: #(postgres_store.empty_aggregate, 0),
      with: fn(aggregate_and_sequence, event_envelop) {
        let #(aggregate, _) = aggregate_and_sequence
        #(
          eventsourcing.Aggregate(
            ..aggregate,
            entity: aggregate.apply(aggregate.entity, event_envelop.payload),
          ),
          event_envelop.sequence,
        )
      },
    )
  eventsourcing.AggregateContext(aggregate_id:, aggregate:, sequence:)
}

fn commit(
  postgres_store: PostgresStore(entity, command, event, error),
  context: eventsourcing.AggregateContext(entity, command, event, error),
  events: List(event),
  metadata: Metadata,
) {
  let eventsourcing.AggregateContext(aggregate_id, _, sequence) = context
  let wrapped_events =
    wrap_events(postgres_store, aggregate_id, events, sequence, metadata)
  persist_events(postgres_store, wrapped_events)
  wrapped_events
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
          metadata: metadata,
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
  wrapped_events: List(eventsourcing.EventEnvelop(event)),
) {
  wrapped_events
  |> list.map(fn(event) {
    let assert eventsourcing.SerializedEventEnvelop(
      aggregate_id,
      sequence,
      payload,
      metadata,
      event_type,
      event_version,
      aggregate_type,
    ) = event

    pog.query(insert_event_query)
    |> pog.parameter(pog.text(aggregate_type))
    |> pog.parameter(pog.text(aggregate_id))
    |> pog.parameter(pog.int(sequence))
    |> pog.parameter(pog.text(event_type))
    |> pog.parameter(pog.text(event_version))
    |> pog.parameter(pog.text(payload |> postgres_store.event_encoder))
    |> pog.parameter(pog.text(metadata |> metadata_encoder))
    |> pog.execute(postgres_store.db)
  })
}

fn metadata_encoder(metadata: Metadata) -> String {
  json.array(metadata, fn(row) {
    json.preprocessed_array([json.string(row.0), json.string(row.1)])
  })
  |> json.to_string
}
