# Contributing to snowflake-sqlalchemy

## Prerequisites

### Python

Python 3.9 or higher is required.

### Hatch

This project uses [Hatch](https://hatch.pypa.io/) for environment management, running tests, and building the package. You can install it once globally:

```bash
pip install hatch
```

Hatch creates and manages isolated virtual environments automatically — you do not need to create or activate one manually.

### Project structure

```
snowflake-sqlalchemy/
├── src/snowflake/sqlalchemy/       # Dialect source code
│   ├── snowdialect.py              # Core dialect implementation
│   ├── custom_types.py             # Snowflake-specific types (VARIANT, VECTOR, DECFLOAT, …)
│   ├── custom_commands.py          # MergeInto, CopyIntoStorage, CreateStage, …
│   ├── alembic_util.py             # Alembic render_item hook for case-sensitive identifiers
│   ├── orm.py                      # SnowflakeBase, SnowflakeSession, snowflake_declarative_base
│   ├── exc.py                      # SnowflakeWarning and other exceptions
│   ├── name_utils.py               # normalize_name / denormalize_name utilities
│   └── sql/custom_schema/          # IcebergTable, HybridTable, DynamicTable, …
├── tests/
│   ├── parameters.py               # Connection credentials — created by you, never committed
│   ├── alembic_integration/        # Alembic-specific integration tests
│   └── sqlalchemy_test_suite/      # SQLAlchemy compliance test suite
├── pyproject.toml                  # Build system, dependencies, hatch scripts, ruff/mypy config
└── tox.ini                         # CI matrix across Python 3.9–3.14
```

Key files:

- **`pyproject.toml`** — the single source of truth for dependencies and tooling. The `[tool.hatch.envs.default.scripts]` table lists all developer commands.
- **`tox.ini`** — used by CI to run the full matrix; mirrors the hatch scripts but covers multiple Python versions.
- **`src/snowflake/sqlalchemy/snowdialect.py`** — the main dialect class; most feature work touches this file.

## Development setup

Clone the repository and let Hatch build the default environment (Python 3.9, all dev and pandas extras):

```bash
git clone https://github.com/snowflakedb/snowflake-sqlalchemy.git
cd snowflake-sqlalchemy
hatch env create
```

Verify the installation:

```bash
hatch run check-import
```

### Pre-commit hooks (recommended)

```bash
hatch run pre-commit install
```

This installs git hooks that run `ruff` (formatting and linting) on every commit so issues are caught before they reach CI.

## Snowflake connection configuration

Integration tests require a live Snowflake account. Create `tests/parameters.py` with your credentials:

```python
CONNECTION_PARAMETERS = {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "database": "your_database",
    "schema": "your_schema",
}
```

This file is listed in `.gitignore` and must never be committed.

## Running tests

### Main test suite

```bash
hatch run test-dialect
```

This runs all tests under `tests/` (excluding the SQLAlchemy compliance suite) with coverage reporting.

### SQLAlchemy compliance suite

```bash
hatch run test-dialect-compatibility
```

The compliance tests live in `tests/sqlalchemy_test_suite/` and are intentionally excluded from the default run — see [`tests/sqlalchemy_test_suite/README.md`](tests/sqlalchemy_test_suite/README.md) for the reason.

### Alembic integration tests

```bash
hatch run pytest tests/alembic_integration/
```

These tests require the `alembic` dependency (included in the `development` extras) and a valid Snowflake connection. See [`tests/alembic_integration/README.md`](tests/alembic_integration/README.md) for details.

### AWS-specific tests

```bash
hatch run test-dialect-aws
```

Runs only tests marked with `@pytest.mark.aws`.

### Filtering tests

Pass any `pytest` arguments after `--`:

```bash
# Run a single file
hatch run pytest tests/test_custom_types.py

# Run with verbose output
hatch run pytest -vv tests/test_core.py
```

## Linting, formatting, and type checking

Run all pre-commit hooks (ruff format + lint) across the whole codebase:

```bash
hatch run check
```

Run mypy type checking:

```bash
hatch run type-check
```

## Building the package

```bash
hatch build
```

The built wheel and sdist appear in `dist/`.

## Submitting a pull request

1. **Open a GitHub issue first** — every PR should reference an issue (`Fixes #NNNN`).
2. **Add tests** — new behaviour should be covered by at least one automated test.
3. **Pass CI** — `hatch run check` and `hatch run test-dialect` must pass locally before you push.
4. Fill out the PR description using the template provided by GitHub, including a short explanation of how your change solves the issue.

For questions or general support, see the [Support](README.md#support) section in the README.
