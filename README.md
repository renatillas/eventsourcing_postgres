<h1 align="center">Eventsourcing Postgres</h1>

<div align="center">
  ✨ <strong>Event Sourcing Library for Gleam with PostgreSQL</strong> ✨
</div>

<div align="center">
  A Gleam library for building event-sourced systems using PostgreSQL as the event store.
</div>

<br />

<div align="center">
  <a href="https://hex.pm/packages/eventsourcing_postgres">
    <img src="https://img.shields.io/hexpm/v/eventsourcing_postgres" alt="Available on Hex" />
  </a>
  <a href="https://hexdocs.pm/eventsourcing_postgres">
    <img src="https://img.shields.io/badge/hex-docs-ffaff3" alt="Documentation" />
  </a>
</div>

---

## Table of contents

- [Introduction](#introduction)
- [Features](#features)
- [Philosophy](#philosophy)
- [Installation](#installation)
- [Support](#support)
- [Contributing](#contributing)
- [License](#license)

## Introduction

`eventsourcing_postgres` is a Gleam library designed to help developers build event-sourced systems using PostgreSQL. Event sourcing is a pattern where changes to the application's state are stored as a sequence of events.

## Features

- **Event Sourcing**: Build systems based on the event sourcing pattern.
- **PostgreSQL Event Store**: Robust event store implementation using PostgreSQL.
- **Command Handling**: Handle commands and produce events with robust error handling.
- **Event Application**: Apply events to update aggregates.
- **Snapshotting**: Optimize aggregate rebuilding with configurable snapshots.
- **Type-safe Error Handling**: Comprehensive error types and Result-based API.
- **Concurrency Safe**: Uses pessimistic concurrency to ensure safe concurrent access.
- **JSONB Support**: Native PostgreSQL JSONB columns for better performance and querying.
- **Database Migration**: Safe migration tools from TEXT to JSONB columns.
- **CLI Tools**: Command-line interface for database management and migrations.

## Example

```gleam
import gleam/io
import eventsourcing_postgres
import eventsourcing 

// First we would define the pog config and all the necessary values for the event sourcing system

let pog_actor_spec = pgo_config
|> pog.supervised()


use _ <- result.try(
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(pog_actor_spec)
  |> supervisor.start()
)


let postgres_store = eventsourcing_postgres.new(
  pgo_config 
  event_encoder 
  event_decoder 
  event_type 
  event_version 
  aggregate_type 
  entity_encoder 
  entity_decoder 
) 

let event_sourcing = 
  eventsourcing.new(
    event_store: postgres_store,
    queries: [query],
    handle: handle,
    apply: apply,
    empty_state: emtpy_state,
  )

```

## CLI Database Management

The library includes a powerful CLI for managing your PostgreSQL event store:

### Quick Setup

```sh
# Create tables with JSONB columns (recommended for new projects)
gleam run create-tables

# Create legacy TEXT tables (for backward compatibility)
gleam run create-legacy-tables
```

### Migration Commands

```sh
# Migrate existing TEXT tables to JSONB (safe, keeps old columns)
gleam run migrate-only

# Finalize migration (removes old TEXT columns - backup first!)
gleam run finalize-migration  

# Complete migration in one step
gleam run migrate
```

### Configuration

Configure database connection using environment variables or `.env` file:

```sh
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=myapp
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### Examples

```sh
# Use with custom database
POSTGRES_DATABASE=production gleam run create-tables

# Migrate production data safely
POSTGRES_DATABASE=production gleam run migrate-only
# ... verify migration success ...
POSTGRES_DATABASE=production gleam run finalize-migration
```

For detailed migration information, see [MIGRATION.md](MIGRATION.md).

## Philosophy

eventsourcing_postgres is designed to make building event-sourced systems easy and intuitive.
It encourages a clear separation between command handling and event application,
making your code more maintainable and testable.

## Installation

Add eventsourcing_postgres to your Gleam projects from the command line:

``` sh
gleam add eventsourcing_postgres@1
```

## Support

eventsourcing_postgres is built by Renatillas.
Contributions are very welcome!
If you've spotted a bug, or would like to suggest a feature,
please open an issue or a pull request.

## Testing

The library includes comprehensive tests to ensure reliability:

```sh
# Run all tests
gleam test

# Run tests with Docker PostgreSQL
docker compose up -d
gleam test
```

**Note**: Some test log entries like `relation "snapshot" does not exist` are expected behavior from tests that intentionally validate error handling when database tables are missing.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch (git checkout -b my-feature-branch).
3. Make your changes and commit them (git commit -m 'Add new feature').
4. Push to the branch (git push origin my-feature-branch).
5. Open a pull request.
6. Please ensure your code adheres to the project's coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
