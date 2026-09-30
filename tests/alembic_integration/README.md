# Alembic Integration Tests

This directory contains integration tests for Alembic migration scenarios with the Snowflake SQLAlchemy dialect.

For development setup and connection configuration, see [CONTRIBUTING.md](../../CONTRIBUTING.md).

## Shared Setup

No shared setup is needed: loading the dialect registers Alembic's Snowflake implementation (`snowflake.sqlalchemy.alembic_impl.SnowflakeImpl`), so direct `MigrationContext.configure(...)` calls work out of the box. Don't define another `SnowflakeImpl` here — it would replace the built-in one and the tests would no longer exercise it.

## Running the Tests

### Run all Alembic integration tests:
```bash
hatch run pytest tests/alembic_integration/
```

### Run a specific test:
```bash
hatch run pytest tests/alembic_integration/test_multi_schema_fk.py::test_alembic_autogenerate_multi_schema_fk
```

### Run with verbose output:
```bash
hatch run pytest -vv tests/alembic_integration/
```
