# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 5.2.0 - 2025-09-05

### Added

- **CLI Tools**: Complete command-line interface for database management
  - `gleam run create-tables` - Create tables with JSONB columns
  - `gleam run create-legacy-tables` - Create tables with TEXT columns
  - `gleam run migrate` - Complete migration from TEXT to JSONB
  - `gleam run migrate-only` - Safe migration keeping old columns
  - `gleam run finalize-migration` - Finalize migration removing old columns
  - `gleam run help` - Show help and usage information

- **JSONB Support**: Native PostgreSQL JSONB column support for better performance
  - `create_event_table()` and `create_snapshot_table()` now create JSONB columns by default
  - Legacy TEXT column functions available as `create_event_table_legacy()` and `create_snapshot_table_legacy()`

- **Database Migration System**: Safe, incremental migration tools
  - `migrate_event_table_to_jsonb()` - Migrate event table to JSONB
  - `migrate_snapshot_table_to_jsonb()` - Migrate snapshot table to JSONB  
  - `finalize_event_table_migration()` - Complete event table migration
  - `finalize_snapshot_table_migration()` - Complete snapshot table migration
  - `migrate_to_jsonb()` - All-in-one migration function
  - `migrate_to_jsonb_safe()` - Safe migration keeping old columns

- **Environment Configuration**: Support for `.env` files and environment variables
  - Database connection configuration via environment variables
  - Automatic `.env` file loading for CLI commands

- **Enhanced Testing**: Comprehensive migration and validation test suites
  - Migration data preservation tests
  - Idempotent migration tests  
  - Legacy vs new table comparison tests

### Changed

- **Dependencies**: Added `argv` and `dot_env` for CLI and environment support
- **Default Behavior**: New installations use JSONB columns by default (breaking change for new users only)

### Fixed

- Fixed test suite duplicate key constraint violation in `migration_validation_test.gleam` by using unique aggregate IDs for legacy vs new table comparisons
- Improved test isolation to prevent database conflicts between concurrent test cases

### Documentation

- Added CLI usage section to README with examples and configuration
- Created comprehensive MIGRATION.md guide for TEXT to JSONB column migration
- Added testing section to README explaining expected database error logs during test execution
- Added notes about intentional "snapshot does not exist" errors in test logs
