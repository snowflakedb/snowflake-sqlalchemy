# Migrating from Snowflake SQLAlchemy 1.x to 2.x

This guide helps you upgrade the **Snowflake SQLAlchemy dialect**
(`snowflake-sqlalchemy`) from the **1.x** line (latest: **1.11.0**) to the
**2.x** line. Two 2.x previews have shipped so far — **2.0.0a0** (the initial 2.x
preview) and **2.0.0a2** (current). This guide targets the latest and notes which
preview introduced each change where it matters.

> **This is not the same as upgrading SQLAlchemy itself.** `snowflake-sqlalchemy`
> 2.x *requires* SQLAlchemy 2.x. If your application still runs on SQLAlchemy 1.4,
> migrate the application to SQLAlchemy 2.0 first — follow
> [SQLAlchemy's 1.4→2.0 migration guide](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html) —
> then upgrade this dialect. Behavior changes that come from SQLAlchemy Core/ORM
> itself (e.g. `Result` rows, `text()` requirements, `Engine` autocommit removal)
> are covered there and are not repeated here.

## Table of contents

- [At a glance](#at-a-glance)
- [Am I affected?](#am-i-affected)
- [Step-by-step upgrade](#step-by-step-upgrade)
- [Requirements changes](#requirements-changes)
- [Breaking changes](#breaking-changes)
- [Behavioral differences to review](#behavioral-differences-to-review)
- [New in 2.x (additive, no migration needed)](#new-in-2x-additive-no-migration-needed)
- [Staying on 1.x](#staying-on-1x)
- [Troubleshooting](#troubleshooting)
- [Further reading](#further-reading)

## At a glance

| Area | 1.x (≤ 1.11.0) | 2.x (latest: 2.0.0a2) |
| --- | --- | --- |
| SQLAlchemy | `>=1.4.19` (1.4 and 2.x both supported) | `>=2.0.0` (2.x only) |
| Python | `>=3.8` | `>=3.10` (through 3.14) |
| `snowflake-connector-python` | `<5.0.0` | `<5.0.0` (unchanged) |
| Public API surface | — | Additive, except the temporary `legacy_url_params` shim (removed) |

**The short version:** for most projects the only required action is to be on
**SQLAlchemy 2.x** and **Python 3.10+**. Two feature-flag defaults flipped in 2.x
(`enable_structured_type_json` → on, `force_div_is_floordiv` → off) and the
temporary `legacy_url_params` shim was removed — review the
[breaking changes](#breaking-changes) and
[behavioral differences](#behavioral-differences-to-review) below.

## Am I affected?

| Your current setup | What you need to do |
| --- | --- |
| Already on SQLAlchemy 2.x + Python 3.10+ | Drop-in upgrade. Then skim [behavioral differences](#behavioral-differences-to-review). |
| On SQLAlchemy 1.4 | Migrate the app to SQLAlchemy 2.0 first, then upgrade. Until then pin `snowflake-sqlalchemy<2.0.0`. |
| On Python 3.9 or older | Upgrade the interpreter to 3.10–3.14 first. |
| Import `snowflake.sqlalchemy.compat` anywhere | Remove that import — the module was deleted (see [Breaking changes](#breaking-changes)). |
| Use `regexp_match(...)` / `regexp_replace(...)` with `flags=` and are coming from a release **older than 1.10.1** | Review generated SQL — flag rendering was fixed (see [behavioral differences](#behavioral-differences-to-review)). |
| Pass `legacy_url_params=` or set `SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS` | Remove it; move blocked connector kwargs to `connect_args=` (see [Breaking changes](#breaking-changes)). |
| Read semi-structured `VARIANT`/`OBJECT`/`ARRAY` values with `json.loads(...)` | Values now come back as `dict`/`list` by default — see [behavioral difference #5](#5-automatic-json-handling-is-on-by-default-enable_structured_type_json). |
| Use `/` or `//` on column expressions and assert the compiled SQL or results | Generated SQL changed — see [behavioral difference #6](#6-division-operators-compile-differently-force_div_is_floordiv). |

## Step-by-step upgrade

1. Make sure your application runs on **SQLAlchemy 2.0** (migrate from 1.4 if
   needed, using SQLAlchemy's guide linked above).
2. Make sure you are on **Python 3.10–3.14**.
3. Upgrade the dialect. The 2.x line is currently a **pre-release**, so pin the
   exact preview version (a plain `>=2.0.0` range skips pre-releases unless you
   add `--pre`):

   ```shell
   pip install "snowflake-sqlalchemy==2.0.0a2"
   # or, to take the newest 2.x pre-release from a range:
   pip install --upgrade --pre "snowflake-sqlalchemy>=2.0.0"
   ```

4. Verify the install:

   ```shell
   python -c "import snowflake.sqlalchemy; print(snowflake.sqlalchemy.__version__)"
   ```

5. Run your test suite and review the [behavioral differences](#behavioral-differences-to-review)
   — in particular reflection-dependent code and any `regexp_*` usage.

See also the README's
[Installing Snowflake SQLAlchemy](README.md#installing-snowflake-sqlalchemy) and
[Verifying Your Installation](README.md#verifying-your-installation).

## Requirements changes

| Requirement | 1.11.0 | 2.0.0a0 | 2.0.0a2 (latest) |
| --- | --- | --- | --- |
| `requires-python` | `>=3.8` | `>=3.9` | `>=3.10` |
| Python classifiers | 3.8–3.13 | 3.9–3.14 | 3.10–3.14 |
| `SQLAlchemy` | `>=1.4.19` | `>=2.0.0` | `>=2.0.0` |
| `snowflake-connector-python` | `<5.0.0` | `<5.0.0` | `<5.0.0` |

The Python floor rose in two steps: 2.0.0a0 dropped 3.8 (floor `>=3.9`), and
2.0.0a2 dropped 3.9 (floor `>=3.10`).

## Breaking changes

### SQLAlchemy 1.4 support dropped

*Since 2.0.0a0.* The dialect now requires `SQLAlchemy>=2.0.0` and is no longer
tested against 1.4. On SQLAlchemy 1.4 the resolver will refuse to install 2.x, or
the dialect will fail at import/runtime.

```diff
  # requirements.txt
- SQLAlchemy>=1.4.19
- snowflake-sqlalchemy<2.0.0
+ SQLAlchemy>=2.0.0
+ snowflake-sqlalchemy>=2.0.0
```

If you cannot move off SQLAlchemy 1.4 yet, pin `snowflake-sqlalchemy<2.0.0`.

### Python 3.8 and 3.9 support dropped

The Python floor rose across the two 2.x previews: **2.0.0a0** raised it to
`>=3.9` (dropping 3.8), and **2.0.0a2** raised it again to `>=3.10` (dropping
3.9). Coming from 1.x you land on the latest, so upgrade the interpreter to
3.10–3.14 before upgrading the dialect.

```diff
  # pyproject.toml
- requires-python = ">=3.8"
+ requires-python = ">=3.10"
```

### Internal `compat` module removed

*Since 2.0.0a0.* `snowflake.sqlalchemy.compat` (the internal SQLAlchemy 1.4/2.0
shim: `IS_VERSION_20`, `args_reducer`, `string_types`, `returns_unicode`) was
deleted. It was never part of the public API, but if you imported it, remove the
import and use SQLAlchemy 2.x APIs directly.

```diff
- from snowflake.sqlalchemy.compat import IS_VERSION_20
```

### `legacy_url_params` removed

Earlier releases (through 2.0.0a0) provided a temporary `legacy_url_params`
opt-out (a `create_engine()` kwarg, or the `SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS`
environment variable) that re-enabled forwarding certain **sensitive connector
parameters** from the URL query string. **This shim was removed in 2.0.0a2:**

- Passing `legacy_url_params=...` to `create_engine()` now raises
  `sqlalchemy.exc.ArgumentError`.
- `SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS` has **no effect**.
- These connector kwargs are **always** rejected from the URL query string and
  must be passed via `connect_args=`:
  `host`, `protocol`, `token_file_path`, `private_key_file`,
  `ocsp_response_cache_filename`, `connection_diag_log_path`, `crl_cache_dir`,
  `unsafe_file_write`, `unsafe_skip_file_permissions_check`.

```diff
  from sqlalchemy import create_engine

- # Before — worked only under the removed shim:
- engine = create_engine(
-     "snowflake://user:pass@account/?protocol=https&token_file_path=/path/to/token",
-     legacy_url_params=True,
- )
+ # After — pass blocked parameters through connect_args=:
+ engine = create_engine(
+     "snowflake://user:pass@account/",
+     connect_args={"protocol": "https", "token_file_path": "/path/to/token"},
+ )
```

Details: [Sensitive connection parameters](README.md#sensitive-connection-parameters)
and [Connection Parameters](README.md#connection-parameters).

## Behavioral differences to review

Most of these require no code changes, but they change observable behavior.
Two of them (#5 automatic JSON handling, #6 division operators) can silently
change query results or values you read back — review each item that applies to
you.

### 1. `UUID` columns reflect to a real type (were `NullType`)

On 2.x, Snowflake `UUID` columns reflect to `sqlalchemy.sql.sqltypes.UUID` with
`as_uuid=False`, so values come back as plain hyphenated **strings**. On
SQLAlchemy 1.4 the generic `UUID` type did not exist and these columns reflected
as `NullType`.

```python
from sqlalchemy.sql.sqltypes import UUID   # SA 2.x

# Reflected UUID values are strings by default:
#   "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
# Opt into uuid.UUID objects with:
Column("id", UUID(as_uuid=True), primary_key=True)
```

Details: [UUID Data Type Support](README.md#uuid-data-type-support).

### 2. Reflection routing is automatic (bulk vs single-table)

SQLAlchemy 2.x distinguishes bulk reflection from single-table inspection at the
framework level, and the dialect routes each to the efficient query path
automatically — no configuration needed:

- `MetaData.reflect()` / `autoload_with=` → one schema-wide query per metadata
  type, cached for all tables.
- `inspector.get_columns("t")` → a single `DESC TABLE` against that table.

```python
metadata.reflect(bind=engine, schema="public")          # bulk: schema-wide, cached
inspector.get_columns("my_table", schema="public")      # single: DESC TABLE
```

Details: [Single-Table vs Multi-Table Reflection Performance](README.md#single-table-vs-multi-table-reflection-performance),
[Cache Column Metadata](README.md#cache-column-metadata),
[Reflecting Large Schemas (10,000+ objects)](README.md#reflecting-large-schemas-10000-objects).

### 3. Reflected type `str()` output includes Snowflake default sizes

Because single-table inspection uses `DESC TABLE`, reflected types now render
Snowflake's resolved default sizes — e.g. `VARCHAR(16777216)` instead of
`VARCHAR`, `BINARY(8388608)` instead of `BINARY`. The type *objects* are
functionally identical; only `str()` differs. Compare types with `isinstance()`,
not string comparison.

### 4. `regexp_match` / `regexp_replace` flags render as literals

Flags passed to `ColumnElement.regexp_match(..., flags=...)` /
`regexp_replace(..., flags=...)` now render as inline string literals, matching
Snowflake's `REGEXP_LIKE(col, pattern, 'i')` /
`REGEXP_REPLACE(col, pattern, repl, 'i')` syntax (previously they were routed
through the bound-parameter pipeline, producing invalid SQL).

This fix also shipped in the 1.x line (1.10.1), so it is only a change if you are
coming from a release **older than 1.10.1**. Snapshot/assertion tests that pinned
the old output will need updating.

### 5. Automatic JSON handling is on by default (`enable_structured_type_json`)

`enable_structured_type_json` now defaults to **`True`** as of **2.0.0a2** (it
defaulted to `False` in 1.x and in 2.0.0a0). It affects **semi-structured,
untyped** `VARIANT`, `OBJECT` and `ARRAY` columns. Typed/structured columns
(`OBJECT(...)` with fields, `ARRAY(<type>)`, `MAP(...)`) are unaffected — they
keep native connector handling.

What changes by default:

- **Reads** return Python objects (`dict`/`list`) instead of raw JSON strings.
- **Writes** accept `dict`/`list` directly; the dialect wraps them with
  `PARSE_JSON(...)` automatically — no manual `func.parse_json(json.dumps(...))`.

```python
import json
from sqlalchemy import func

# Reads — raw JSON string in 1.x; already a dict/list in 2.x:
payload = json.loads(row.v)   # old: needed json.loads on a JSON string
payload = row.v               # new: row.v is already a dict/list

# Writes — manual wrapping in 1.x; pass the object directly in 2.x:
stmt = t.insert().values(v=func.parse_json(json.dumps({"a": 1})))   # old
stmt = t.insert().values(v={"a": 1})                                # new
```

Restore the old behavior explicitly (deprecated — emits a `DeprecationWarning`):

```python
create_engine(URL(...), enable_structured_type_json=False)
# or: create_engine("snowflake://...?enable_structured_type_json=false")
```

Details: [Automatic JSON handling with `enable_structured_type_json`](README.md#automatic-json-handling-with-enable_structured_type_json).

### 6. Division operators compile differently (`force_div_is_floordiv`)

`force_div_is_floordiv` now defaults to **`False`** as of **2.0.0a2** (it
defaulted to `True` in 1.x and in 2.0.0a0). This changes the **generated SQL**
for `/` and `//` on SQLAlchemy column expressions:

| Python expression | Old default (`=True`) | New default (`=False`) |
| --- | --- | --- |
| `col1 / col2` | `col1 / CAST(col2 AS NUMERIC)` | `col1 / col2` |
| `col1 // col2` | `col1 / col2` | `FLOOR(col1 / col2)` |

- `/` now performs standard true division (Snowflake `/` already returns a
  fractional result) and no longer casts the denominator to `NUMERIC`.
- `//` now correctly floors; previously it compiled to plain `/`, which was
  **not** floor division.

Restore the old behavior explicitly (deprecated — emits a `DeprecationWarning`):

```python
create_engine(URL(...), force_div_is_floordiv=True)
```

Details: SQLAlchemy's
[true-division change](https://docs.sqlalchemy.org/en/20/changelog/whatsnew_20.html#python-division-operator-performs-true-division-for-all-backends-added-floor-division).

## New in 2.x (additive, no migration needed)

Aside from the temporary `legacy_url_params` shim
([removed](#legacy_url_params-removed)), nothing was removed from the public API.
These exports were added along the 1.x→2.x line and are safe to adopt — no code
changes required to keep working:

- `create_snowflake_engine`
- `SnowflakeBase`, `SnowflakeSession`, `snowflake_declarative_base` — faster bulk
  inserts for ORM models with nullable columns; see
  [Bulk Insert Optimization for ORM Models](README.md#bulk-insert-optimization-for-orm-models).
- `GCSBucket`, `CloudStorageLocation` — see
  [CopyIntoStorage Support](README.md#copyintostorage-support).
- Secret-redaction helpers: `SnowflakeSecretRedactionFilter`,
  `add_secret_redaction_filter`, `redact_secrets`.
- `UUID` re-export.

## Staying on 1.x

If you cannot upgrade yet, pin the major version:

```text
# requirements.txt
snowflake-sqlalchemy<2.0.0
```

The 1.x line remains supported per Snowflake's
[recommended client versions](https://docs.snowflake.com/en/release-notes/requirements#recommended-client-versions)
policy.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| Resolver installs an old dialect or errors on 1.4 | Ensure the app is on SQLAlchemy 2.0, then pin `snowflake-sqlalchemy>=2.0.0`. |
| Resolver refuses to install 2.x on Python 3.9 | Upgrade to Python 3.10–3.14 first, then install the pinned preview: `pip install "snowflake-sqlalchemy==2.0.0a2"`. |
| `ImportError: ... snowflake.sqlalchemy.compat` | Remove the import (see [Breaking changes](#breaking-changes)). |
| `ArgumentError` mentions `legacy_url_params` was removed | Remove `legacy_url_params=` and pass blocked connector kwargs via `connect_args=` — see [`legacy_url_params` removed](#legacy_url_params-removed). |
| Reflected UUID values are strings, not `uuid.UUID` | Expected; use `UUID(as_uuid=True)` — see [behavioral difference #1](#1-uuid-columns-reflect-to-a-real-type-were-nulltype). |
| Type-string comparisons fail after reflection (`VARCHAR(16777216)`) | Use `isinstance()` — see [behavioral difference #3](#3-reflected-type-str-output-includes-snowflake-default-sizes). |
| `REGEXP_*` SQL changed | Expected; see [behavioral difference #4](#4-regexp_match--regexp_replace-flags-render-as-literals). |
| Semi-structured reads return `dict`/`list`, or `json.loads(...)` now raises `TypeError` | Expected; drop the manual JSON parsing, or opt out with `enable_structured_type_json=False` (deprecated) — see [behavioral difference #5](#5-automatic-json-handling-is-on-by-default-enable_structured_type_json). |
| `/` or `//` compiled SQL / results changed | Expected; update expectations, or set `force_div_is_floordiv=True` (deprecated) — see [behavioral difference #6](#6-division-operators-compile-differently-force_div_is_floordiv). |

## Further reading

- [Connection Parameters](README.md#connection-parameters) and
  [Sensitive connection parameters](README.md#sensitive-connection-parameters)
  (some connector kwargs must be passed via `connect_args` since 1.11.0).
- [Object Name Case Handling](README.md#object-name-case-handling) and
  [Case-sensitive identifiers](README.md#case-sensitive-identifiers).
- [Auto-increment Behavior](README.md#auto-increment-behavior) and
  [Identity columns as primary keys](README.md#identity-columns-as-primary-keys).
- [VARIANT, ARRAY and OBJECT Support](README.md#variant-array-and-object-support)
  and [Structured Data Types Support](README.md#structured-data-types-support).
- [Alembic Support](README.md#alembic-support).
- [Known Limitations](README.md#known-limitations).
- SQLAlchemy's own [1.4→2.0 migration guide](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html).
