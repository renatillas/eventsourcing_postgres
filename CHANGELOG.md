# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 5.2.0 - 2024-06-15

### Fixed

- Fixed test suite duplicate key constraint violation in `migration_validation_test.gleam` by using unique aggregate IDs for legacy vs new table comparisons
- Improved test isolation to prevent database conflicts between concurrent test cases

### Documentation

- Added testing section to README explaining expected database error logs during test execution
- Created comprehensive MIGRATION.md guide for TEXT to JSONB column migration
- Added notes about intentional "snapshot does not exist" errors in test logs

