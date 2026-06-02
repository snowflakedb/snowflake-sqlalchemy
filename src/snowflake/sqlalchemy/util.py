#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import os
import re
import warnings
from itertools import chain
from typing import Any
from urllib.parse import quote as _url_quote
from urllib.parse import quote_plus, urlsplit, urlunsplit

from sqlalchemy import create_engine as _sa_create_engine
from sqlalchemy import exc, inspection, sql
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoForeignKeysError
from sqlalchemy.orm.util import _ORMJoin as sa_orm_util_ORMJoin
from sqlalchemy.sql.base import _expand_cloned, _from_objects
from sqlalchemy.sql.elements import AsBoolean, ClauseElement, True_, _find_columns
from sqlalchemy.sql.selectable import FromClause, Join, Lateral

from snowflake.connector.compat import IS_STR
from snowflake.connector.connection import SnowflakeConnection

from ._constants import (
    APPLICATION_NAME,
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
    SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)


def _rfc_1738_quote(text: str) -> str:
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


# --- connection-input handling --------------------------------------------
# Rules for caller-controlled connection inputs: the URL-authority allowlist
# (account/region), the denylist of connector kwargs that must not arrive via
# the URL query string, and the shared reject-or-warn gate.

# account/region are DNS labels; reject characters that would act as URL
# delimiters in the authority component.
_SAFE_URL_FIELD_RE = re.compile(r"^[A-Za-z0-9._-]+$")

# Connector kwargs that must travel via connect_args= rather than the URL query
# string (they change the connection target, read/write local files, or relax a
# connector safety check). Re-exported from snowdialect for backwards compat.
_URL_QUERY_BLOCKED_KWARGS: frozenset = frozenset(
    {
        "host",
        "protocol",
        "token_file_path",
        "private_key_file",
        "ocsp_response_cache_filename",
        "connection_diag_log_path",
        "crl_cache_dir",
        "unsafe_file_write",
        "unsafe_skip_file_permissions_check",
    }
)


def _reject_or_warn(message: str, *, legacy: bool, stacklevel: int = 2) -> None:
    """Raise ``ArgumentError``, or warn under the legacy shim. ``stacklevel`` is
    relative to this helper's caller."""
    if legacy:
        # +1 skips this helper's own frame so the warning is attributed to the
        # caller that supplied ``stacklevel``.
        warnings.warn(message, DeprecationWarning, stacklevel=stacklevel + 1)
    else:
        raise exc.ArgumentError(message)


def _validate_url_field(field: str, value: str) -> None:
    if not _SAFE_URL_FIELD_RE.fullmatch(value):
        # The builder has no engine, so only the env var can relax this.
        _reject_or_warn(
            f"'{field}' contains characters that cannot be safely placed in the "
            f"connection URL: {value!r}. "
            "Only alphanumeric characters, hyphens, dots, and underscores are allowed. "
            "To restore the previous behaviour temporarily, set the "
            f"{SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS} environment variable.",
            legacy=_legacy_url_params_enabled(),
            stacklevel=3,  # caller's URL(...) site
        )


def _url(**db_parameters: Any) -> str:
    """
    Composes a SQLAlchemy connect string from the given database connection
    parameters.

    Password containing special characters (e.g., '@', '%') need to be encoded to be parsed correctly.
    Unescaped password containing special characters might lead to authentication failure.
    Please follow the instructions to encode the password:
    https://github.com/snowflakedb/snowflake-sqlalchemy#escaping-special-characters-such-as---signs-in-passwords
    """
    specified_parameters: list[str] = []
    if "account" not in db_parameters:
        raise exc.ArgumentError("account parameter must be specified.")

    # Percent-encode user so that metacharacters (@, ?, #, …) cannot corrupt the
    # URL authority component.  SQLAlchemy decodes the userinfo field when it
    # parses the URL, so the connector always receives the original plain value.
    user = _url_quote(db_parameters.get("user", ""), safe="")

    if "host" in db_parameters:
        ret = "snowflake://{user}:{password}@{host}:{port}/".format(
            user=user,
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            host=db_parameters["host"],
            port=db_parameters["port"] if "port" in db_parameters else 443,
        )
        specified_parameters += ["user", "password", "host", "port"]
    elif "region" not in db_parameters:
        account = db_parameters["account"]
        _validate_url_field("account", account)
        ret = "snowflake://{user}:{password}@{account}/".format(
            account=account,
            user=user,
            password=_rfc_1738_quote(db_parameters.get("password", "")),
        )
        specified_parameters += ["user", "password", "account"]
    else:
        account = db_parameters["account"]
        region = db_parameters["region"]
        _validate_url_field("account", account)
        _validate_url_field("region", region)
        ret = "snowflake://{user}:{password}@{account}.{region}/".format(
            account=account,
            user=user,
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            region=region,
        )
        specified_parameters += ["user", "password", "account", "region"]

    if "database" in db_parameters:
        ret += quote_plus(db_parameters["database"])
        specified_parameters += ["database"]
        if "schema" in db_parameters:
            ret += "/" + quote_plus(db_parameters["schema"])
            specified_parameters += ["schema"]
    elif "schema" in db_parameters:
        raise exc.ArgumentError("schema cannot be specified without database")

    def sep(is_first_parameter: bool) -> str:
        return "?" if is_first_parameter else "&"

    is_first_parameter = True
    for p in sorted(db_parameters.keys()):
        v = db_parameters[p]
        if p not in specified_parameters:
            encoded_value = quote_plus(v) if IS_STR(v) else str(v)
            ret += sep(is_first_parameter) + p + "=" + encoded_value
            is_first_parameter = False
    return ret


def _set_connection_interpolate_empty_sequences(
    dbapi_connection: SnowflakeConnection, flag: bool
) -> None:
    """set the _interpolate_empty_sequences config of the underlying connection"""
    if hasattr(dbapi_connection, "driver_connection"):
        # _dbapi_connection is a _ConnectionFairy which proxies raw SnowflakeConnection
        dbapi_connection.driver_connection._interpolate_empty_sequences = flag
    else:
        # _dbapi_connection is a raw SnowflakeConnection
        dbapi_connection._interpolate_empty_sequences = flag  # type: ignore[attr-defined]


def _update_connection_application_name(**conn_kwargs: Any) -> dict[str, Any]:
    if PARAM_APPLICATION not in conn_kwargs:
        conn_kwargs[PARAM_APPLICATION] = APPLICATION_NAME
    if PARAM_INTERNAL_APPLICATION_NAME not in conn_kwargs:
        conn_kwargs[PARAM_INTERNAL_APPLICATION_NAME] = APPLICATION_NAME
    if PARAM_INTERNAL_APPLICATION_VERSION not in conn_kwargs:
        conn_kwargs[PARAM_INTERNAL_APPLICATION_VERSION] = SNOWFLAKE_SQLALCHEMY_VERSION
    return conn_kwargs


def parse_url_boolean(value: str) -> bool:
    if value.lower() in ("true", "1"):
        return True
    elif value.lower() in ("false", "0"):
        return False
    else:
        raise ValueError(f"Invalid boolean value detected: '{value}'")


def parse_url_integer(value: str) -> int:
    try:
        return int(value)
    except ValueError as e:
        raise ValueError(f"Invalid int value detected: '{value}") from e


def _legacy_url_params_enabled() -> bool:
    """Whether the legacy URL-params compatibility shim is enabled.

    Reuses :func:`parse_url_boolean` so the env variable is interpreted exactly
    like every other boolean flag in the dialect (``true``/``1``, case-insensitive).
    An unset, empty, or unrecognised value disables the shim rather than raising.
    """
    value = os.environ.get(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, "")
    try:
        return parse_url_boolean(value)
    except ValueError:
        return False


# handle Snowflake BCR bcr-1057
# the BCR impacts sqlalchemy.orm.context.ORMSelectCompileState and sqlalchemy.sql.selectable.SelectState
# which used the 'sqlalchemy.util.preloaded.sql_util.find_left_clause_to_join_from' method that
# can not handle the BCR change, we implement it in a way that lateral join does not need onclause
def _find_left_clause_to_join_from(
    clauses: list[FromClause],
    join_to: Any,
    onclause: ClauseElement | None,
) -> range | list[int]:
    """Given a list of FROM clauses, a selectable,
    and optional ON clause, return a list of integer indexes from the
    clauses list indicating the clauses that can be joined from.

    The presence of an "onclause" indicates that at least one clause can
    definitely be joined from; if the list of clauses is of length one
    and the onclause is given, returns that index.   If the list of clauses
    is more than length one, and the onclause is given, attempts to locate
    which clauses contain the same columns.

    """
    idx: list[int] = []
    selectables = set(_from_objects(join_to))

    # if we are given more than one target clause to join
    # from, use the onclause to provide a more specific answer.
    # otherwise, don't try to limit, after all, "ON TRUE" is a valid
    # on clause
    if len(clauses) > 1 and onclause is not None:
        resolve_ambiguity = True
        cols_in_onclause = _find_columns(onclause)
    else:
        resolve_ambiguity = False
        cols_in_onclause = None

    for i, f in enumerate(clauses):
        for s in selectables.difference([f]):
            if resolve_ambiguity:
                assert cols_in_onclause is not None  # set when resolve_ambiguity=True
                if set(f.c).union(s.c).issuperset(cols_in_onclause):
                    idx.append(i)
                    break
            elif onclause is not None or Join._can_join(f, s):
                idx.append(i)
                break
            elif onclause is None and isinstance(s, Lateral):
                # in snowflake, onclause is not accepted for lateral due to BCR change:
                # https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
                # sqlalchemy only allows join with on condition.
                # to adapt to snowflake syntax change,
                # we make the change such that when oncaluse is None and the right part is
                # Lateral, we append the index indicating Lateral clause can be joined from with without onclause.
                idx.append(i)
                break

    if len(idx) > 1:
        # this is the same "hide froms" logic from
        # Selectable._get_display_froms
        toremove = set(chain(*[_expand_cloned(f._hide_froms) for f in clauses]))
        idx = [i for i in idx if clauses[i] not in toremove]

    # onclause was given and none of them resolved, so assume
    # all indexes can match
    if not idx and onclause is not None:
        return range(len(clauses))
    else:
        return idx


class _Snowflake_Selectable_Join(Join):
    """Join subclass for Snowflake BCR-1057 (lateral joins without ON clause)."""

    def _match_primaries(self, left: FromClause, right: FromClause) -> Any:
        try:
            return super()._match_primaries(left, right)
        except NoForeignKeysError:
            if isinstance(right, Lateral):
                # BCR-1057: lateral joins don't require FK relationships
                return None
            raise


class _Snowflake_ORMJoin(_Snowflake_Selectable_Join, sa_orm_util_ORMJoin):
    """_ORMJoin subclass for Snowflake BCR-1057 (lateral joins without ON clause).

    Inherits ``_match_primaries`` from ``_Snowflake_Selectable_Join`` via MRO so
    lateral joins without FK relationships don't raise ``NoForeignKeysError``.

    ``_ORMJoin.__init__`` asserts ``self.onclause is not None`` immediately after
    calling ``Join.__init__``, so for the lateral-without-ON case we pass a
    ``sql.true()`` placeholder to satisfy the assertion. If no other criteria
    are applied, ``self.onclause`` still holds just the placeholder and we reset
    it to ``None`` so compilation emits ``JOIN LATERAL ...`` without an ON
    clause as Snowflake's BCR-1057 requires.
    """

    def __init__(
        self,
        left: Any,
        right: Any,
        onclause: Any | None = None,
        isouter: bool = False,
        full: bool = False,
        _left_memo: Any | None = None,
        _right_memo: Any | None = None,
        _extra_criteria: Any = (),
    ) -> None:
        is_lateral_without_onclause = onclause is None and isinstance(
            inspection.inspect(right).selectable, Lateral
        )
        super().__init__(
            left,
            right,
            onclause=sql.true() if is_lateral_without_onclause else onclause,
            isouter=isouter,
            full=full,
            _left_memo=_left_memo,
            _right_memo=_right_memo,
            _extra_criteria=_extra_criteria,
        )

        if is_lateral_without_onclause and _is_true_placeholder(self.onclause):
            self.onclause = None


def _is_true_placeholder(onclause: Any) -> bool:
    """Return True if ``onclause`` is only the ``sql.true()`` placeholder that
    ``_Snowflake_ORMJoin`` passes through ``_ORMJoin.__init__`` to satisfy its
    ``onclause is not None`` assertion.

    Join coercions wrap ``sql.true()`` in an :class:`AsBoolean` envelope, so the
    placeholder shows up as ``AsBoolean(True_)`` rather than a bare ``True_``.
    """
    if isinstance(onclause, True_):
        return True
    if isinstance(onclause, AsBoolean) and isinstance(onclause.element, True_):
        return True
    return False


def create_snowflake_engine(
    base_url: str,
    schema: str | None = None,
    case_sensitive_schema: bool = False,
    **kwargs: Any,
) -> Engine:
    """
    Create a Snowflake SQLAlchemy engine with optional case-sensitive schema support.

    When ``case_sensitive_schema=True`` the schema name is wrapped in URL-encoded
    double-quotes (``%22``) so that Snowflake treats the name as case-sensitive.
    ``create_connect_args`` calls ``unquote_plus`` on the database/schema
    component, which turns ``%22myschema%22`` back into ``'"myschema"'`` (with
    literal double-quotes) before forwarding to the Snowflake connector.

    Parameters
    ----------
    base_url:
        A Snowflake SQLAlchemy URL string of the form
        ``snowflake://user:password@account/database``.  Must not end with a
        trailing slash unless no database is specified.
    schema:
        Optional schema name to append to the URL.
    case_sensitive_schema:
        When *True* the schema name is enclosed in ``%22...%22`` to preserve
        case in Snowflake.  Defaults to *False*.
    **kwargs:
        Additional keyword arguments forwarded verbatim to
        :func:`sqlalchemy.create_engine`.

    Returns
    -------
    sqlalchemy.engine.Engine
    """
    if schema is not None:
        if case_sensitive_schema:
            schema_part = f"%22{_url_quote(schema, safe='')}%22"
        else:
            schema_part = _url_quote(schema, safe="")
        # Use urlsplit/urlunsplit to safely insert schema into path before query params
        parsed = urlsplit(base_url)
        path = parsed.path.rstrip("/")
        if path.count("/") >= 2:
            raise ValueError(
                f"base_url already contains a schema component: {base_url!r}. "
                "base_url must be in the form 'snowflake://user:pass@account/database' "
                "with no trailing schema segment."
            )
        new_path = f"{path}/{schema_part}"
        url = urlunsplit(
            (parsed.scheme, parsed.netloc, new_path, parsed.query, parsed.fragment)
        )
    else:
        url = base_url
    return _sa_create_engine(url, **kwargs)


def escape_backslashes(value: str) -> str:
    """Double backslashes so they survive Snowflake's ESCAPE_STRING_LITERALS.

    Snowflake interprets backslash escape sequences inside string literals by
    default, so any literal backslash in user data must be doubled.
    """
    return value.replace("\\", "\\\\")


def escape_string_literal_interior(value: str) -> str:
    """Escape the interior of a single-quoted Snowflake string literal: double
    single quotes (standard SQL ``''``) and backslashes (Snowflake
    ``ESCAPE_STRING_LITERALS``). Returns the interior only — no surrounding
    quotes — and does not double percent signs, so it is safe to interpolate
    into a ``%``-formatted DDL template.
    """
    return escape_single_quotes(value).replace("\\", "\\\\")


def escape_single_quotes(value: str) -> str:
    """Double single quotes only, leaving backslashes untouched.

    For single-quoted string-literal options where Snowflake backslash
    sequences (``\\n``, ``\\134``, ``\\N``) must be preserved verbatim — unlike
    ``escape_string_literal_interior``, which also doubles backslashes.
    """
    return value.replace("'", "''")


# --- identifier quoting primitives -----------------------------------------
#
# Pure, config-free helpers shared by ``SnowflakeIdentifierPreparer`` (base.py)
# and ``_NameUtils`` (name_utils.py).  Kept here, decoupled from the preparer,
# so they can be unit-tested directly without constructing a dialect.  The
# Snowflake-specific config (reserved words, illegal-identifier sets, the
# legal-character regex, the case-sensitivity flag) stays on the preparer /
# dialect and is passed in by the callers.


def split_identifier_parts(text: str):
    """Split a dotted identifier string into ``(value, was_quoted)`` parts.

    Splits on unquoted dots while honouring double-quoted segments, so
    ``"db.schema"`` -> ``[("db", False), ("schema", False)]`` and the quoted
    ``'"my.schema"'`` -> ``[("my.schema", True)]``.  A doubled quote inside a
    quoted segment (``"a""b"``) is unescaped to a single ``"`` (-> ``a"b``).

    ``was_quoted`` records whether the part was enclosed in double quotes in the
    source string; the caller decides what quoting that implies.  Returns only
    the raw parts — **no** ``quoted_name`` wrapping and **no** case-sensitivity
    policy (that lives in the preparer, which knows the dialect flag).

    Parts must be dot-separated.  A quoted segment adjacent to other text without
    a separating dot (``prefix"X"`` / ``"X"suffix``) or an unterminated quote is
    malformed and raises ``ValueError`` rather than being parsed into an
    arbitrary multi-part reference.
    """
    ret = []
    idx = 0
    pre_idx = 0
    in_quote = False
    while idx < len(text):
        if not in_quote:
            if text[idx] == "." and pre_idx < idx:
                ret.append((text[pre_idx:idx], False))
                pre_idx = idx + 1
            elif text[idx] == '"':
                # A quoted segment starts a part: unquoted text right before it
                # (no separating dot) is malformed.
                if pre_idx < idx:
                    raise ValueError(
                        f"invalid identifier {text!r}: unquoted text is adjacent "
                        'to a quoted segment without a separating "."'
                    )
                in_quote = True
                pre_idx = idx + 1
        else:
            if text[idx] == '"':
                # "" inside a quoted segment is an escaped literal " character
                # (e.g. "my""schema" -> my"schema).
                if idx + 1 < len(text) and text[idx + 1] == '"':
                    idx += 1  # skip the second quote; keep accumulating
                else:
                    value = text[pre_idx:idx].replace('""', '"')
                    ret.append((value, True))
                    in_quote = False
                    pre_idx = idx + 1
                    # A quoted segment ends a part: text other than a dot right
                    # after the closing quote (no separating dot) is malformed.
                    if idx + 1 < len(text) and text[idx + 1] != ".":
                        raise ValueError(
                            f"invalid identifier {text!r}: a quoted segment is "
                            'adjacent to further text without a separating "."'
                        )
        idx += 1
        if pre_idx < len(text) and text[pre_idx] == ".":
            pre_idx += 1
    if in_quote:
        raise ValueError(f"invalid identifier {text!r}: unterminated quoted segment")
    if pre_idx < idx:
        ret.append((text[pre_idx:idx], False))
    return ret


def requires_quotes(
    value: str,
    *,
    include_case: bool = True,
    reserved_words,
    illegal_identifiers,
    illegal_initial_characters,
    legal_characters,
) -> bool:
    """Return True if ``value`` requires double-quoting.

    Structural triggers (reserved word, illegal identifier, illegal initial
    character, or any character outside ``legal_characters``) always apply. With
    ``include_case`` (the default) an upper/mixed-case identifier also requires
    quotes to preserve its case against Snowflake's folding. Pass
    ``include_case=False`` for the structural-only check, where a bare uppercase
    name is fine because Snowflake folds it.
    """
    if not value:
        return False
    lc_value = value.lower()
    structural = (
        lc_value in reserved_words
        or lc_value in illegal_identifiers
        or value[0] in illegal_initial_characters
        or not legal_characters.match(str(value))
    )
    return structural or (include_case and lc_value != value)
