# Migrating from Snowflake SQLAlchemy 1.x to 2.x

This guide helps you upgrade the **Snowflake SQLAlchemy dialect**
(`snowflake-sqlalchemy`) from any **1.x** release to **2.0.0**. Most changes apply
the same way whichever 1.x release you start from; the few that landed partway
through 1.x are called out with the release that introduced them, so check those
against your current version.

> **This is not the same as upgrading SQLAlchemy itself.** `snowflake-sqlalchemy`
> 2.x *requires* SQLAlchemy 2.0. If your application still runs on SQLAlchemy 1.4,
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
- [New in 2.0 (additive, no migration needed)](#new-in-20-additive-no-migration-needed)
- [Staying on 1.x](#staying-on-1x)
- [Troubleshooting](#troubleshooting)
- [Further reading](#further-reading)

## At a glance

| Area | 1.x (latest 1.11.1) | 2.0.0 |
| --- | --- | --- |
| SQLAlchemy | `>=1.4.19` (1.4 and 2.0 both supported) | `>=2.0.44,<2.1` (2.0 only) |
| Python | `>=3.8` | `>=3.11` (through 3.14) |
| `snowflake-connector-python` | `<5.0.0` | `>=5.0.0` (5.x required) |
| asyncio | Not supported | `create_async_engine()` / `AsyncSession` supported |
| Removed options | — | `legacy_url_params`, `force_div_is_floordiv` |

**The short version:** be on **SQLAlchemy 2.0.44+ (below 2.1)**, **Python 3.11+**
and **connector 5.x**. One feature-flag default changed
(`enable_structured_type_json` is on, so semi-structured values are read and
written as `dict`/`list`), and two options were removed (`legacy_url_params`,
`force_div_is_floordiv`) — review the [breaking changes](#breaking-changes) and
[behavioral differences](#behavioral-differences-to-review) below.

## Am I affected?

| Your current setup | What you need to do |
| --- | --- |
| Already on SQLAlchemy 2.0.44+ (below 2.1) and Python 3.11+ | Drop-in upgrade. Then skim [behavioral differences](#behavioral-differences-to-review). |
| On SQLAlchemy 2.0 older than 2.0.44, or on 2.1 | Move SQLAlchemy into `>=2.0.44,<2.1` — 2.1 breaks `import snowflake.sqlalchemy`. |
| On SQLAlchemy 1.4 | Migrate the app to SQLAlchemy 2.0 first, then upgrade. Until then pin `snowflake-sqlalchemy<2.0.0`. |
| On Python 3.10 or older | Upgrade the interpreter to 3.11–3.14 first. |
| Pin `snowflake-connector-python<5.0.0` yourself | Remove the upper bound — 2.0 requires connector 5.x (see [Breaking changes](#breaking-changes)). |
| Import `snowflake.sqlalchemy.compat` anywhere | Remove that import — the module was deleted (see [Breaking changes](#breaking-changes)). |
| Pass `unsafe_skip_config_file_permissions_check` or `crl_unsafe_skip_file_permissions_check` in the URL query string | Move them to `connect_args=` — they are now blocked from the URL (see [`legacy_url_params` removed](#legacy_url_params-removed)). |
| Pass `legacy_url_params=` or set `SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS` | Remove it; move blocked connector kwargs to `connect_args=` (see [Breaking changes](#breaking-changes)). |
| Pass `force_div_is_floordiv=` | Remove it — the option was removed (see [behavioral difference #4](#4-force_div_is_floordiv-removed)). |
| Use `//` on integer columns and are coming from a release **older than 1.11.1** | Results change: `//` now floors — see [behavioral difference #4](#4-force_div_is_floordiv-removed). |
| Pass `host`, `protocol`, `private_key_file` or other sensitive connector parameters in the URL query string and are coming from a release **older than 1.11.0** | Move them to `connect_args=` (see [`legacy_url_params` removed](#legacy_url_params-removed)). |
| Use `regexp_match(...)` / `regexp_replace(...)` with `flags=` and are coming from a release **older than 1.10.1** | Review generated SQL — see [behavioral difference #8](#8-regexp_match--regexp_replace-flags-render-as-literals). |
| Read semi-structured `VARIANT`/`OBJECT`/`ARRAY` values with `json.loads(...)`, or write them with `PARSE_JSON` | Values now come back as `dict`/`list` and can be written directly — see [behavioral difference #5](#5-automatic-json-handling-is-on-by-default-enable_structured_type_json). |
| Read or write typed `MAP(...)` / `ARRAY(<type>)` / `OBJECT(...)` columns | These also return/accept `dict`/`list` by default — see [behavioral difference #5](#5-automatic-json-handling-is-on-by-default-enable_structured_type_json). |
| Store `decimal.Decimal` values inside `VARIANT`/`OBJECT`/`ARRAY` payloads | They are stored as JSON strings — see [behavioral difference #6](#6-decimal-values-in-semi-structured-payloads-are-stored-as-json-strings). |

## Step-by-step upgrade

1. Make sure your application runs on **SQLAlchemy 2.0.44 or newer, below 2.1**
   (migrate from 1.4 if needed, using SQLAlchemy's guide linked above).
2. Make sure you are on **Python 3.11–3.14**.
3. Remove any `snowflake-connector-python<5.0.0` pin of your own.
4. Upgrade the dialect:

   ```shell
   pip install --upgrade "snowflake-sqlalchemy>=2.0.0,<3"
   ```

5. Verify the install:

   ```shell
   python -c "import snowflake.sqlalchemy; print(snowflake.sqlalchemy.__version__)"
   ```

6. Run your test suite and review the [behavioral differences](#behavioral-differences-to-review)
   — in particular semi-structured (JSON) reads and writes.

See also the README's
[Installing Snowflake SQLAlchemy](README.md#installing-snowflake-sqlalchemy) and
[Verifying Your Installation](README.md#verifying-your-installation).

## Requirements changes

| Requirement | 1.x (latest 1.11.1) | 2.0.0 |
| --- | --- | --- |
| `requires-python` | `>=3.8` | `>=3.11` |
| Python classifiers | 3.8–3.13 | 3.11–3.14 |
| `SQLAlchemy` | `>=1.4.19` | `>=2.0.44,<2.1` |
| `snowflake-connector-python` | `<5.0.0` | `>=5.0.0` |

The `<2.1` SQLAlchemy ceiling exists because SQLAlchemy 2.1.0 removed APIs the
dialect imports at module load time, so `import snowflake.sqlalchemy` fails
outright on 2.1. If you pin SQLAlchemy yourself, keep it in `>=2.0.44,<2.1`.

## Breaking changes

### SQLAlchemy 1.4 support dropped

The dialect requires `SQLAlchemy>=2.0.44,<2.1` and is no longer tested against
1.4. On SQLAlchemy 1.4 the resolver will refuse to install 2.0.

```diff
  # requirements.txt
- SQLAlchemy>=1.4.19
- snowflake-sqlalchemy<2.0.0
+ SQLAlchemy>=2.0.44,<2.1
+ snowflake-sqlalchemy>=2.0.0
```

If you cannot move off SQLAlchemy 1.4 yet, pin `snowflake-sqlalchemy<2.0.0`.

### Python 3.8, 3.9, and 3.10 support dropped

The dialect requires Python 3.11+, because `snowflake-connector-python` 5.x itself
requires Python 3.11+ — pip refuses to install it (or this dialect) on older
interpreters. Upgrade the interpreter to 3.11–3.14 before upgrading the dialect.

```diff
  # pyproject.toml
- requires-python = ">=3.8"
+ requires-python = ">=3.11"
```

### `snowflake-connector-python` 5.x required

The dialect requires `snowflake-connector-python>=5.0.0`; connector 4.x is no
longer supported. Connector 5.x is what provides the async driver behind the new
[async support](#async-support).

```diff
  # requirements.txt
- snowflake-connector-python<5.0.0
+ snowflake-connector-python>=5.0.0
```

If you pin the connector yourself, remove any `<5.0.0` upper bound or the resolver
will not be able to satisfy both packages.

### Internal `compat` module removed

`snowflake.sqlalchemy.compat` (the internal SQLAlchemy 1.4/2.0 shim:
`IS_VERSION_20`, `args_reducer`, `string_types`, `returns_unicode`) was deleted.
It was never part of the public API, but if you imported it, remove the import and
use SQLAlchemy 2.0 APIs directly.

```diff
- from snowflake.sqlalchemy.compat import IS_VERSION_20
```

### `legacy_url_params` removed

Since **1.11.0**, sensitive connector parameters are blocked from the URL query
string. 1.11.x provided a temporary `legacy_url_params` opt-out (a `create_engine()`
kwarg, or the `SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS` environment variable) that
re-enabled forwarding certain **sensitive connector parameters** from the URL
query string. **This opt-out was removed in 2.0:**

- Passing `legacy_url_params=...` as a `create_engine()` keyword raises
  `TypeError: Invalid argument(s) 'legacy_url_params' sent to create_engine()`.
  A `?legacy_url_params=...` URL query parameter is not rejected, but it has no
  effect either.
- `SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS` has **no effect**.
- These connector kwargs are **always** rejected from the URL query string and
  must be passed via `connect_args=`:
  `host`, `protocol`, `token_file_path`, `private_key_file`,
  `ocsp_response_cache_filename`, `connection_diag_log_path`, `crl_cache_dir`,
  `unsafe_file_write`, `unsafe_skip_file_permissions_check`,
  `unsafe_skip_config_file_permissions_check`, `crl_unsafe_skip_file_permissions_check`.
  The last two are new in 2.0. If you are coming from a release older than 1.11.0,
  all of them are newly blocked.

```diff
  from sqlalchemy import create_engine

- # Before — worked only under the removed opt-out:
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
#5 (automatic JSON handling) and #6 (`Decimal` in payloads) can change the values
you read back — review each item that applies to you.

Items #1 and #2 only apply if you are coming from **SQLAlchemy 1.4**; on 1.x with
SQLAlchemy 2.0 you already have this behavior. Item #3 also applies on SQLAlchemy
2.0 if you are coming from a release older than 1.10.0.

### 1. `UUID` columns reflect to a real type (were `NullType`)

On SQLAlchemy 2.0, Snowflake `UUID` columns reflect to
`sqlalchemy.sql.sqltypes.UUID` with `as_uuid=False`, so values come back as plain
hyphenated **strings**. On SQLAlchemy 1.4 the generic `UUID` type did not exist
and these columns reflected as `NullType`.

```python
from sqlalchemy.sql.sqltypes import UUID   # SA 2.0

# Reflected UUID values are strings by default:
#   "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
# Opt into uuid.UUID objects with:
Column("id", UUID(as_uuid=True), primary_key=True)
```

Details: [UUID Data Type Support](README.md#uuid-data-type-support).

### 2. Reflection routing is automatic (bulk vs single-table)

SQLAlchemy 2.0 distinguishes bulk reflection from single-table inspection at the
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

*On SQLAlchemy 2.0, since 1.10.0.* Because single-table inspection uses `DESC TABLE`, reflected types render
Snowflake's resolved default sizes — e.g. `VARCHAR(16777216)` instead of
`VARCHAR`, `BINARY(8388608)` instead of `BINARY`. The type *objects* are
functionally identical; only `str()` differs. Compare types with `isinstance()`,
not string comparison.

### 4. `force_div_is_floordiv` removed

The `force_div_is_floordiv` dialect option was **removed**. `/` and `//` always
compile as follows, with no option to change it:

| Python expression | Generated SQL | Behaviour |
| --- | --- | --- |
| `col1 / col2` | `col1 / col2` | True division (Snowflake's native `/`) |
| `col1 // col2` | `FLOOR(col1 / col2)` | Floor division |

What this means depends on the 1.x release you are coming from:

- **1.11.1:** no change — it already compiles `/` and `//` this way.
- **Older than 1.11.1, on SQLAlchemy 2.0** (with the default
  `force_div_is_floordiv=True`): `//` between two integer operands compiled to a
  plain `col1 / col2`, which Snowflake evaluates as true division. It now compiles
  to `FLOOR(col1 / col2)`, so **integer `//` results change**. `/` previously
  rendered as `col1 / CAST(col2 AS NUMERIC)`; results are the same, only the SQL
  text differs.

Passing `force_div_is_floordiv` as a `create_engine()` keyword raises
`TypeError: Invalid argument(s) 'force_div_is_floordiv' sent to create_engine()`
(a `?force_div_is_floordiv=...` URL query parameter is not rejected, but has no
effect). Remove it from your connection configuration:

```python
# Before (removed — raises TypeError)
create_engine(URL(...), force_div_is_floordiv=True)

# After
create_engine(URL(...))
```

Details: SQLAlchemy's
[true-division change](https://docs.sqlalchemy.org/en/20/changelog/whatsnew_20.html#python-division-operator-performs-true-division-for-all-backends-added-floor-division).

### 5. Automatic JSON handling is on by default (`enable_structured_type_json`)

The new `enable_structured_type_json` dialect flag is **on by default**. It
affects semi-structured `VARIANT`, `OBJECT` and `ARRAY` columns and typed
structured columns (`MAP(...)`, `ARRAY(<type>)`, `OBJECT(...)` with fields).

What changes by default:

- **Reads** return Python objects (`dict`/`list`) instead of raw JSON strings.
- **Writes** accept `dict`/`list` directly; the dialect wraps them with
  `PARSE_JSON(...)` automatically — no manual `func.parse_json(json.dumps(...))`.
  For typed columns it renders `CAST(PARSE_JSON(...) AS <declared type>)`.
- Element values of typed columns are **not** converted to the declared types on
  read — they come back as JSON decodes them (for example `MAP(NUMBER, ...)` keys
  stay strings and `DATE` elements stay ISO strings).

```python
import json
from sqlalchemy import func

# Reads — raw JSON string in 1.x; already a dict/list in 2.0:
payload = json.loads(row.v)   # old: needed json.loads on a JSON string
payload = row.v               # new: row.v is already a dict/list

# Writes — manual wrapping in 1.x; pass the object directly in 2.0:
stmt = t.insert().values(v=func.parse_json(json.dumps({"a": 1})))   # old
stmt = t.insert().values(v={"a": 1})                                # new
```

Restore the old behavior explicitly (deprecated — emits a `DeprecationWarning`):

```python
create_engine(URL(...), enable_structured_type_json=False)
# or: create_engine("snowflake://...?enable_structured_type_json=false")
```

Details: [Automatic JSON handling with `enable_structured_type_json`](README.md#automatic-json-handling-with-enable_structured_type_json).

### 6. `Decimal` values in semi-structured payloads are stored as JSON strings

When the dialect serializes a `dict`/`list` for a `VARIANT`, `OBJECT` or `ARRAY`
column, `decimal.Decimal` values are written as JSON **strings** (preserving all
38 `DECFLOAT` digits, which a JSON number would truncate). `TYPEOF(col:key)`
therefore reports `VARCHAR`. To read one back as a number, cast through `VARCHAR`
first — Snowflake rejects a direct cast of the `PARSE_JSON` text to `DECFLOAT`:

```sql
SELECT payload:amt::VARCHAR::DECFLOAT FROM t;   -- works
SELECT payload:amt::DECFLOAT FROM t;            -- fails: Cannot cast value of type TEXT ... to DECFLOAT
```

Details: [DECFLOAT Data Type Support](README.md#decfloat-data-type-support).

### 7. Keep-alive auto-detection is pinned on new connections

The dialect explicitly sets the connector parameter
`enable_server_session_keep_alive_auto_detection=True` on every new connection.
This pins the connector's *current* default rather than changing behavior: the
connector emits a `FutureWarning` because its default is slated to change, so
pinning it keeps behavior stable across that change and silences the warning.

No action is needed. A value you supply yourself — via the URL query string or
`connect_args=` — always takes precedence.

### 8. `regexp_match` / `regexp_replace` flags render as literals

*Since 1.10.1* — only a change if you are coming from an older release. Flags
passed to `ColumnElement.regexp_match(..., flags=...)` /
`regexp_replace(..., flags=...)` render as inline string literals, matching
Snowflake's `REGEXP_LIKE(col, pattern, 'i')` /
`REGEXP_REPLACE(col, pattern, repl, 'i')` syntax (previously they were routed
through the bound-parameter pipeline, producing invalid SQL). Snapshot/assertion
tests that pinned the old output will need updating.

## New in 2.0 (additive, no migration needed)

Aside from the [removed options](#breaking-changes), nothing was removed from the
public API. The additions below are safe to adopt — no code changes are required
to keep working.

### Async support

The dialect implements SQLAlchemy's
[`asyncio` extension](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html),
so you can use `AsyncEngine` / `AsyncSession` for non-blocking access. This is
purely additive — existing sync code is unaffected.

There is **no separate `+driver` suffix**: async uses the same `snowflake://` URL as
the sync dialect, and SQLAlchemy selects the async dialect when you call
`create_async_engine()`. It needs [`greenlet`](https://pypi.org/project/greenlet/),
so install the `[async]` extra: `pip install "snowflake-sqlalchemy[async]"`.
Without it, async engine creation raises an `ImportError` with install
instructions.

> **Upgrading an async app from 2.0.0 to 2.1:** snowflake-sqlalchemy 2.0.0 capped
> SQLAlchemy below 2.1, where greenlet still came by default on common
> platforms. snowflake-sqlalchemy 2.1 allows SQLAlchemy 2.1, which no longer
> installs greenlet, so an upgrade that does not add the `[async]` extra
> installs cleanly but fails at async engine creation. Switch your requirement to
> `snowflake-sqlalchemy[async]`.

```python
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("snowflake://user:password@account/")
```

Details: [Async Support](README.md#async-support).

### Native `UUID` with `enable_native_uuid` (opt-in)

SQLAlchemy's generic `Uuid` type (and `Mapped[uuid.UUID]`) still renders as
`CHAR(32)` by default. Setting `enable_native_uuid=True` renders it as native
`UUID` instead, so tables created before you enable it no longer match their model
definition — plan a migration before turning it on. No change unless you opt in.

Details: [Native `UUID` with `enable_native_uuid`](README.md#native-uuid-with-enable_native_uuid).

### Strict host-name mode (opt-in)

`strict_host_names=True` (or the `SNOWFLAKE_SQLALCHEMY_STRICT_HOST_NAMES`
environment variable) stops the dialect from inferring or appending domains to host
names and forwards connection parameters to the connector unvalidated — intended for
air-gapped deployments. No change unless you opt in.

Details: [Strict host-name mode](README.md#strict-host-name-mode).

### Other additions

See the [2.0.0 release notes](DESCRIPTION.md) for the full list, including
`COLLATE` support, subscript access on semi-structured columns, `InsertMulti`
(`INSERT ALL` / `INSERT FIRST`), row access policies on `SnowflakeTable`, and
`RELY` constraints.

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
| Resolver installs an old dialect or errors on 1.4 | Ensure the app is on SQLAlchemy 2.0.44+ (below 2.1), then install `snowflake-sqlalchemy>=2.0.0`. |
| `import snowflake.sqlalchemy` fails with an `ImportError` from `sqlalchemy.orm` | You are on SQLAlchemy 2.1; downgrade to `>=2.0.44,<2.1`. |
| Resolver refuses to install 2.0 on Python 3.10 or older | Upgrade to Python 3.11–3.14 first, then `pip install "snowflake-sqlalchemy>=2.0.0"`. |
| Resolver cannot satisfy `snowflake-connector-python` | Remove any `<5.0.0` connector pin of your own — 2.0 requires connector 5.x; see [`snowflake-connector-python` 5.x required](#snowflake-connector-python-5x-required). |
| `ImportError: ... snowflake.sqlalchemy.compat` | Remove the import (see [Breaking changes](#breaking-changes)). |
| `TypeError: Invalid argument(s) 'legacy_url_params' sent to create_engine()` | Remove `legacy_url_params=` and pass blocked connector kwargs via `connect_args=` — see [`legacy_url_params` removed](#legacy_url_params-removed). |
| `TypeError: Invalid argument(s) 'force_div_is_floordiv' sent to create_engine()` | Remove the argument — see [behavioral difference #4](#4-force_div_is_floordiv-removed). |
| Integer `//` results changed | Expected when coming from a release older than 1.11.1; `//` now floors — see [behavioral difference #4](#4-force_div_is_floordiv-removed). |
| `ArgumentError` about a connection parameter that cannot be set via the URL query string | Pass it via `connect_args=` — see [`legacy_url_params` removed](#legacy_url_params-removed). |
| Reflected UUID values are strings, not `uuid.UUID` | Expected; use `UUID(as_uuid=True)` — see [behavioral difference #1](#1-uuid-columns-reflect-to-a-real-type-were-nulltype). |
| Type-string comparisons fail after reflection (`VARCHAR(16777216)`) | Use `isinstance()` — see [behavioral difference #3](#3-reflected-type-str-output-includes-snowflake-default-sizes). |
| Semi-structured reads return `dict`/`list`, or `json.loads(...)` now raises `TypeError` | Expected; drop the manual JSON parsing, or opt out with `enable_structured_type_json=False` (deprecated) — see [behavioral difference #5](#5-automatic-json-handling-is-on-by-default-enable_structured_type_json). |

## Further reading

- [Connection Parameters](README.md#connection-parameters) and
  [Sensitive connection parameters](README.md#sensitive-connection-parameters).
- [Object Name Case Handling](README.md#object-name-case-handling) and
  [Case-sensitive identifiers](README.md#case-sensitive-identifiers).
- [Auto-increment Behavior](README.md#auto-increment-behavior) and
  [Identity columns as primary keys](README.md#identity-columns-as-primary-keys).
- [VARIANT, ARRAY and OBJECT Support](README.md#variant-array-and-object-support)
  and [Structured Data Types Support](README.md#structured-data-types-support).
- [Async Support](README.md#async-support).
- [Alembic Support](README.md#alembic-support).
- [Known Limitations](README.md#known-limitations).
- SQLAlchemy's own [1.4→2.0 migration guide](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html).
