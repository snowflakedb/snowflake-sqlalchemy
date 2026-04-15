# Alembic Integration Tests

This directory contains integration tests for Alembic migration scenarios with the Snowflake SQLAlchemy dialect.

## Prerequisites

These tests require Alembic, which is included in the `development` optional dependencies defined in `pyproject.toml`.

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

## Connection Requirements

These tests require a valid Snowflake connection configured in `tests/parameters.py`. The tests will use the connection parameters to create temporary schemas and tables, which are cleaned up automatically after each test.
