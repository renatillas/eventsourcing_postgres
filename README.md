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
- [Concurrency Safety](#concurrency-safety)
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

## Concurrency Safety

### What is Pessimistic Concurrency?

Pessimistic concurrency is a method of managing concurrent access to a resource by locking it when a transaction is being performed. This ensures that no other transaction can modify the resource until the lock is released.

### Why is it Important?

In the context of event sourcing, multiple transactions could attempt to modify the same aggregate simultaneously. Without proper concurrency control, this could lead to inconsistencies in the stored events. Pessimistic concurrency ensures that only one transaction can modify an aggregate at any given time, preserving data integrity and consistency.

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
