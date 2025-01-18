import eventsourcing
import gleam/bool
import gleam/dynamic
import gleam/json
import gleam/list
import gleam/pair
import gleam/pgo
import gleam/result

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
    db: pgo.Connection,
    empty_aggregate: eventsourcing.Aggregate(entity, command, event, error),
    event_encoder: fn(event) -> String,
    event_decoder: fn(String) -> Result(event, List(dynamic.DecodeError)),
    event_type: String,
    event_version: String,
    aggregate_type: String,
  )
}

// CONSTRUCTORS ----

pub fn new(
  pgo_config pgo_config: pgo.Config,
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
    Result(event, List(dynamic.DecodeError)),
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
  let db = pgo.connect(pgo_config)

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
  )
}

pub fn create_event_table(
  postgres_store: PostgresStore(entity, command, event, error),
) -> Result(pgo.Returned(dynamic.Dynamic), pgo.QueryError) {
  pgo.execute(
    query: create_event_table_query,
    on: postgres_store.db,
    with: [],
    expecting: dynamic.dynamic,
  )
}

pub fn load_aggregate_entity(
  postgres_store: PostgresStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> Result(entity, Nil) {
  let assert Ok(commited_events) = load_events(postgres_store, aggregate_id)
  use <- bool.guard(commited_events |> list.length == 0, Error(Nil))
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
  Ok(
    eventsourcing.AggregateContext(aggregate_id:, aggregate:, sequence:).aggregate.entity,
  )
}

pub fn load_events(
  postgres_store: PostgresStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> Result(List(eventsourcing.EventEnvelop(event)), pgo.QueryError) {
  use resulted <- result.map(pgo.execute(
    select_events_query,
    on: postgres_store.db,
    with: [pgo.text(postgres_store.aggregate_type), pgo.text(aggregate_id)],
    expecting: dynamic.decode7(
      eventsourcing.SerializedEventEnvelop,
      dynamic.element(1, dynamic.string),
      dynamic.element(2, dynamic.int),
      dynamic.element(5, fn(dyn) {
        let assert Ok(payload) =
          dynamic.string(dyn) |> result.map(postgres_store.event_decoder)
        payload
      }),
      dynamic.element(6, metadata_decoder),
      dynamic.element(4, dynamic.string),
      dynamic.element(0, dynamic.string),
      dynamic.element(3, dynamic.string),
    ),
  ))
  resulted.rows
}

fn load_aggregate(
  postgres_store: PostgresStore(entity, command, event, error),
  aggregate_id: eventsourcing.AggregateId,
) -> eventsourcing.AggregateContext(entity, command, event, error) {
  let assert Ok(commited_events) = load_events(postgres_store, aggregate_id)

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

    pgo.execute(
      query: insert_event_query,
      on: postgres_store.db,
      with: [
        pgo.text(aggregate_type),
        pgo.text(aggregate_id),
        pgo.int(sequence),
        pgo.text(event_type),
        pgo.text(event_version),
        pgo.text(payload |> postgres_store.event_encoder),
        pgo.text(metadata |> metadata_encoder),
      ],
      expecting: dynamic.dynamic,
    )
  })
}

fn metadata_encoder(metadata: Metadata) -> String {
  json.array(metadata, fn(row) {
    json.preprocessed_array([json.string(row.0), json.string(row.1)])
  })
  |> json.to_string
}

fn metadata_decoder(
  dyn_metadata: dynamic.Dynamic,
) -> Result(Metadata, List(dynamic.DecodeError)) {
  use str_metadata <- result.try(dynamic.string(dyn_metadata))
  use list_metadata <- result.map(
    json.decode(
      str_metadata,
      dynamic.list(of: dynamic.list(of: dynamic.string)),
    )
    |> result.map_error(fn(_) { panic }),
  )
  list.map(list_metadata, fn(metadata) {
    case metadata {
      [key, value] -> #(key, value)
      _ -> panic
    }
  })
}
