# Alembic Integration Tests

This directory contains integration tests for Alembic with the Snowflake SQLAlchemy dialect, specifically testing multi-schema foreign key reflection.

## What These Tests Cover

The tests in this directory validate that Alembic's autogenerate feature correctly handles foreign keys across multiple schemas in Snowflake. This addresses issue #610 where same-schema foreign keys in non-default schemas were incorrectly reflected with a `referred_schema` value, causing Alembic to generate spurious migrations.

### Test Scenarios

1. **Same-schema FKs in non-default schemas**: Verifies that FKs within the same schema (but not the default schema) have `referred_schema=None`
2. **Cross-schema FKs**: Verifies that FKs between different schemas correctly set `referred_schema`
3. **FKs to default schema**: Verifies that FKs pointing to the default schema have `referred_schema=None`
4. **Alembic autogenerate**: Validates that autogenerate only detects actual schema changes, not spurious FK/table operations

## Prerequisites

These tests require Alembic to be installed:

```bash
pip install alembic
```

The tests will be automatically skipped if Alembic is not installed, using `pytest.importorskip("alembic")`.

## Running the Tests

### Run all Alembic integration tests:
```bash
hatch run pytest tests/alembic_integration/
```

### Run a specific test:
```bash
hatch run pytest tests/alembic_integration/test_multi_schema_fk.py::test_alembic_reflection_same_schema_fk
```

### Run with verbose output:
```bash
hatch run pytest -vv tests/alembic_integration/
```

## Test Structure

- `conftest.py`: Provides fixtures for multi-schema test setup and cleanup
- `test_multi_schema_fk.py`: Contains the actual test cases

## How the Tests Work

1. **Setup**: Creates temporary test schemas with unique names (using UUIDs)
2. **Table Creation**: Creates tables with various FK relationships in different schemas
3. **Reflection Testing**: Uses SQLAlchemy's inspector to verify `referred_schema` values
4. **Alembic Testing**: Uses `alembic.autogenerate.compare_metadata()` to verify no spurious operations
5. **Cleanup**: Automatically drops test schemas after each test

## Connection Requirements

These tests require a valid Snowflake connection configured in `tests/parameters.py`. The tests will use the connection parameters to:
- Create temporary schemas
- Create tables with foreign keys
- Test reflection and Alembic autogenerate behavior

## Troubleshooting

If tests fail with connection errors:
1. Ensure `tests/parameters.py` exists with valid Snowflake credentials
2. Verify the account has permissions to create/drop schemas and tables
3. Check that the default schema specified in parameters.py exists

If tests are skipped:
1. Install Alembic: `pip install alembic`
2. To run without Alembic (only reflection tests), remove the `@pytest.mark.skipif` decorator temporarily
