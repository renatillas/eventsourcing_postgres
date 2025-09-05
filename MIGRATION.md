# Migration Guide: TEXT to JSONB Columns

This guide explains how to migrate your existing `eventsourcing_postgres` database from using TEXT columns to proper PostgreSQL JSONB columns for better performance and native JSON operations.

## Benefits of JSONB Migration

- **Better Performance**: JSONB is stored in a binary format and supports efficient indexing
- **Native JSON Operations**: Use PostgreSQL's built-in JSON operators and functions
- **Data Validation**: PostgreSQL validates JSON structure automatically
- **Compression**: JSONB typically uses less storage space than equivalent TEXT
- **Query Performance**: Support for GIN indexes on JSON data

## Important: Data Preservation

✅ **All existing data is preserved during migration**  
✅ **Existing applications continue to work unchanged**  
✅ **Migration can be run multiple times safely**  
✅ **Rollback is possible before finalization**  

The migration is designed to be **safe and reversible** with these steps:

### Step 1: Add JSONB Columns and Convert Data

```gleam
// This adds new JSONB columns alongside existing TEXT columns
// and converts all existing data
let _ = eventsourcing_postgres.migrate_event_table_to_jsonb(postgres_store.eventstore)
let _ = eventsourcing_postgres.migrate_snapshot_table_to_jsonb(postgres_store.eventstore)
```

This step:
- Adds `payload_json` and `metadata_json` columns to the `event` table
- Adds `entity_json` column to the `snapshot` table  
- Converts all existing TEXT data to JSONB format
- **Keeps original TEXT columns intact** for rollback safety

### Step 2: Verify and Finalize (Optional)

After verifying the migration worked correctly:

```gleam  
// WARNING: This permanently removes the old TEXT columns
let _ = eventsourcing_postgres.finalize_event_table_migration(postgres_store.eventstore)
let _ = eventsourcing_postgres.finalize_snapshot_table_migration(postgres_store.eventstore)
```

This step:
- Verifies all data was successfully migrated
- Drops the old TEXT columns (`payload`, `metadata`, `entity`)
- Renames JSONB columns to the original names
- **Cannot be undone** - backup your database first!

### All-in-One Migration

For automated deployments, use the convenience function:

```gleam
// Runs both steps automatically
let _ = eventsourcing_postgres.migrate_to_jsonb(postgres_store.eventstore)
```

## Application Code Changes

**Good news**: Your application code doesn't need to change!

The library continues to work exactly the same way:
- Use `pog.text(json.to_string(data))` to write JSON
- Use `decode.string` to read JSON
- PostgreSQL automatically handles TEXT ↔ JSONB conversion

## New Projects

For new projects, skip migration and use JSONB from the start:

```gleam
// Create tables with JSONB columns directly
let _ = eventsourcing_postgres.create_event_table_with_json(postgres_store.eventstore)  
let _ = eventsourcing_postgres.create_snapshot_table_with_json(postgres_store.eventstore)
```

## SQL Schema Changes

### Before Migration (TEXT columns):
```sql
CREATE TABLE event (
  -- ... other columns ...
  payload   text NOT NULL,
  metadata  text NOT NULL
);

CREATE TABLE snapshot (
  -- ... other columns ...  
  entity    text NOT NULL
);
```

### After Migration (JSONB columns):
```sql
CREATE TABLE event (
  -- ... other columns ...
  payload   jsonb NOT NULL,
  metadata  jsonb NOT NULL
);

CREATE TABLE snapshot (
  -- ... other columns ...
  entity    jsonb NOT NULL  
);
```

## Rollback Procedure

If you need to rollback after Step 1 (before finalizing):

```sql
-- The old TEXT columns are still there, just drop the new JSONB columns
ALTER TABLE event DROP COLUMN payload_json, DROP COLUMN metadata_json;
ALTER TABLE snapshot DROP COLUMN entity_json;
```

## Performance Considerations

- The migration converts data in a single transaction per table
- For large datasets, consider running during low-traffic periods
- Test the migration on a copy of your production data first
- Monitor disk space usage during migration (temporary increase expected)

## Error Handling

The migration functions return detailed error information:
- Invalid JSON in existing TEXT columns will cause migration to fail
- All-or-nothing transaction ensures data consistency
- Migration can be safely retried if it fails

## Index Recommendations (Post-Migration)

After migration, consider adding JSON-specific indexes:

```sql
-- Example: Index on specific JSON fields for better query performance
CREATE INDEX idx_event_payload_type ON event USING GIN ((payload->>'event-type'));
CREATE INDEX idx_metadata_keys ON event USING GIN (metadata);
```

## Verification Queries

To verify migration success:

```sql
-- Check that all rows have non-null JSONB data
SELECT COUNT(*) FROM event WHERE payload_json IS NULL OR metadata_json IS NULL;
SELECT COUNT(*) FROM snapshot WHERE entity_json IS NULL;

-- Compare TEXT and JSONB data (should be identical)
SELECT payload = payload_json::text, metadata = metadata_json::text 
FROM event LIMIT 10;
```

Both queries should return 0 rows and TRUE values respectively.