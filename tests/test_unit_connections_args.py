#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Tests for connection arguments and URL handling.
#
# These tests document the expected behaviour of the strict URL/connection
# parameter handling: validated URL authority fields, percent-encoded user, and
# sensitive connector kwargs that must travel via connect_args= rather than the
# URL query string.
#

import re

import pytest
from sqlalchemy import exc
from sqlalchemy.engine.url import URL as SAUrl
from sqlalchemy.engine.url import make_url

from snowflake.connector.connection import DEFAULT_CONFIGURATION
from snowflake.sqlalchemy import URL, base
from snowflake.sqlalchemy._constants import SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS
from snowflake.sqlalchemy.snowdialect import _URL_QUERY_BLOCKED_KWARGS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dialect():
    return base.dialect()


def _url_query(raw_url: str) -> dict:
    """Return the query-param dict for a raw URL string."""
    return dict(make_url(raw_url).query)


def _url_netloc(raw_url: str) -> str:
    """Return the netloc (authority) part of a raw URL string."""
    from urllib.parse import urlsplit

    return urlsplit(raw_url).netloc


# Every connector kwarg that is not settable via the URL query string, paired
# with a representative value.  Shared by the rejection tests (secure mode →
# must raise) and the legacy-mode regression tests (flag set → must warn and
# forward).  Mirrors snowdialect._URL_QUERY_BLOCKED_KWARGS.
_ALL_BLOCKED_PARAMS = [
    ("host", "other.example.com"),
    ("protocol", "http"),
    ("token_file_path", "/etc/passwd"),
    ("private_key_file", "/root/.ssh/id_rsa"),
    ("ocsp_response_cache_filename", "/tmp/ocsp.cache"),
    ("connection_diag_log_path", "/tmp/diag.log"),
    ("crl_cache_dir", "/tmp/crl"),
    ("unsafe_file_write", "true"),
    ("unsafe_skip_file_permissions_check", "true"),
]

# Blocked kwargs that were introduced in connector 4.x and are legitimately
# absent from DEFAULT_CONFIGURATION when running against connector 3.x.
# The dialect still blocks them on 3.x (harmless) so they stay in
# _URL_QUERY_BLOCKED_KWARGS; this set lets the integrity test below skip the
# DEFAULT_CONFIGURATION presence check for version-specific entries.
_BLOCKED_KWARGS_4X_ONLY: frozenset = frozenset({"crl_cache_dir"})


class TestURLFieldEncoding:
    """URL() must reject or encode account/user/region values that contain
    URL metacharacters so that no unintended query parameters are introduced.

    Example handled here:
        URL(account='x?host=other.example.com&account=x&protocol=http&z=',
            user='svc_user', password='SuperSecret!', database='DB', warehouse='WH')
    must not produce a DSN whose query string carries extra params that
    create_connect_args would then forward to the connector.
    """

    @pytest.fixture(autouse=True)
    def _unset_legacy_flag(self, monkeypatch):
        """Run every test in this class with the strict (non-legacy) code path."""
        monkeypatch.delenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, raising=False)

    # --- account fields -----------------------------------------------------

    def test_account_with_question_mark_raises_or_is_encoded(self):
        """account containing '?' must not introduce query parameters."""
        metachar_value = "x?host=other.example.com&account=x&protocol=http&z="
        try:
            result = URL(
                account=metachar_value,
                user="svc_user",
                password="SuperSecret!",
                database="DB",
                warehouse="WH",
            )
        except exc.ArgumentError:
            return  # Raising is the preferred behaviour

        # If it didn't raise, no extra params may appear in the query string
        parsed = make_url(result)
        q = dict(parsed.query)
        assert "host" not in q, (
            "account value introduced 'host' query param: "
            f"URL was {result!r}, query={q}"
        )
        assert "protocol" not in q, (
            "account value introduced 'protocol' query param: "
            f"URL was {result!r}, query={q}"
        )

    def test_account_with_at_sign_raises_or_is_safe(self):
        """account containing '@' would change the userinfo/host split."""
        try:
            result = URL(
                account="x@other.com",
                user="svc",
                password="pw",
            )
        except exc.ArgumentError:
            return

        # Must not produce a netloc whose host part includes other.com
        netloc = _url_netloc(result)
        assert (
            "other.com" not in netloc.split("@")[-1]
        ), f"@ in account changed the host to include other.com; netloc={netloc!r}"

    def test_account_with_slash_raises_or_is_safe(self):
        """account containing '/' would change the path component."""
        try:
            result = URL(account="x/y", user="u", password="pw")
        except exc.ArgumentError:
            return

        parsed = make_url(result)
        # The path must refer only to the database and schema, not extra segments
        assert parsed.host not in (
            "",
            None,
        ), f"slash in account broke host parsing; got URL {result!r}"

    def test_account_with_ampersand_raises_or_is_safe(self):
        """account containing '&' must not introduce extra query params."""
        try:
            result = URL(account="acct&warehouse=OTHER", user="u", password="pw")
        except exc.ArgumentError:
            return

        q = _url_query(result)
        assert (
            "warehouse" not in q or q.get("warehouse") != "OTHER"
        ), f"& in account introduced warehouse query param; query={q}"

    # --- user field ---------------------------------------------------------

    def test_user_with_at_sign_is_encoded(self):
        """user containing '@' must be percent-encoded so the host is preserved."""
        result = URL(account="myaccount", user="alice@example.com", password="pw")

        # The host derived from account must still be myaccount (+ .snowflakecomputing.com)
        netloc = _url_netloc(result)
        host_part = netloc.split("@")[-1]
        assert (
            "example.com" not in host_part
        ), f"@ in user changed the host; netloc={netloc!r}"

    def test_user_with_question_mark_is_encoded(self):
        """user containing '?' must be encoded, not treated as start of query string."""
        result = URL(account="acct", user="user?host=other.com", password="pw")

        # The extra parameter must not appear as a real query param
        q = _url_query(result)
        assert (
            "host" not in q
        ), f"'?' in user introduced 'host' query param; URL was {result!r}, query={q}"

    def test_user_with_hash_is_encoded(self):
        """user containing '#' must be encoded so it doesn't start a fragment."""
        result = URL(account="acct", user="user#fragment", password="pw")

        from urllib.parse import urlsplit

        parsed_raw = urlsplit(result)
        assert (
            parsed_raw.fragment == ""
        ), f"'#' in user leaked into URL fragment; URL was {result!r}"

    # --- region field -------------------------------------------------------

    def test_region_with_question_mark_raises_or_is_safe(self):
        """region containing '?' must not introduce query parameters."""
        try:
            result = URL(
                account="acct",
                user="u",
                password="pw",
                region="us-east-1?host=other.com",
            )
        except exc.ArgumentError:
            return

        q = _url_query(result)
        assert (
            "host" not in q
        ), f"'?' in region introduced 'host' query param; query={q}"

    # --- valid inputs still work --------------------------------------------

    def test_normal_account_is_unchanged(self):
        """A plain account locator (alphanum + hyphens + dots) must pass through."""
        result = URL(account="xy12345.us-east-1", user="admin", password="pw")
        assert "xy12345.us-east-1" in result

    def test_normal_user_with_no_special_chars_is_unchanged(self):
        """A plain username must be unaffected by any encoding."""
        result = URL(account="acct", user="svc_user_01", password="pw")
        assert "svc_user_01" in result

    def test_normal_region_is_unchanged(self):
        """A plain region string (e.g. eu-central-1) must pass through."""
        result = URL(account="acct", user="u", password="pw", region="eu-central-1")
        assert "eu-central-1" in result

    def test_combined_metachars_account_is_rejected(self):
        """End-to-end check: an account combining several URL metacharacters is
        rejected at build time.

        With the strict code path (legacy flag unset, enforced by the autouse
        fixture above) URL() rejects the account outright, so the extra
        host/protocol values never reach create_connect_args or the connector.
        """
        with pytest.raises(exc.ArgumentError, match="(?i)account"):
            URL(
                account="x?host=other.example.com&account=x&protocol=http&z=",
                user="svc_user",
                password="SuperSecret!",
                database="DB",
                warehouse="WH",
            )


class TestSensitiveParamsRequireConnectArgs:
    """Sensitive connector kwargs must be supplied via connect_args=,
    not the URL query string.

    create_connect_args does not accept query parameters that change the
    connection target or transport, read or write local files, or relax a
    connector safety check — directing callers to the connect_args= path, which
    is under the application's control.
    """

    @pytest.fixture(autouse=True)
    def _unset_legacy_flag(self, monkeypatch):
        """Run every test in this class with the strict (non-legacy) code path."""
        monkeypatch.delenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, raising=False)

    # --- sensitive params are rejected from the URL query -------------------

    @pytest.mark.parametrize("param,value", _ALL_BLOCKED_PARAMS)
    def test_sensitive_query_param_is_rejected(self, param, value):
        """Each sensitive kwarg must raise ArgumentError when set via the URL query
        string in the strict (non-legacy) mode.

        Covers connection target (host), transport (protocol), local file reads
        (token_file_path, private_key_file), local file writes
        (ocsp_response_cache_filename, connection_diag_log_path, crl_cache_dir),
        and connector safety flags (unsafe_file_write,
        unsafe_skip_file_permissions_check).
        """
        dialect = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={param: value},
        )
        with pytest.raises(exc.ArgumentError, match="(?i)" + re.escape(param)):
            dialect.create_connect_args(url)

    # --- combined params -----------------------------------------------------

    def test_combined_sensitive_params_are_rejected(self):
        """A URL combining several sensitive params (host + oauth +
        token_file_path) must still be rejected."""
        dialect = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={
                "host": "other.example.com",
                "authenticator": "oauth",
                "token_file_path": "/var/run/secrets/kubernetes.io/serviceaccount/token",
            },
        )
        with pytest.raises(exc.ArgumentError):
            dialect.create_connect_args(url)

    # --- legitimate params still work ---------------------------------------

    def test_safe_query_params_warehouse_and_role_are_forwarded(self):
        """Ordinary params (warehouse, role) must still work."""
        dialect = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="my-account",
            query={"warehouse": "COMPUTE_WH", "role": "ANALYST"},
        )
        _, opts = dialect.create_connect_args(url)
        assert opts.get("warehouse") == "COMPUTE_WH"
        assert opts.get("role") == "ANALYST"

    def test_safe_query_param_account_is_forwarded(self):
        """account in query (used with explicit host= in URL) must still work."""
        dialect = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="my-account.snowflakecomputing.com",
            query={"account": "my-account"},
        )
        _, opts = dialect.create_connect_args(url)
        assert opts.get("account") == "my-account"

    def test_safe_query_param_authenticator_externalbrowser_is_forwarded(self):
        """authenticator=externalbrowser must still reach the connector."""
        dialect = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="",
            host="my-account",
            query={"authenticator": "externalbrowser"},
        )
        _, opts = dialect.create_connect_args(url)
        assert opts.get("authenticator") == "externalbrowser"

    def test_safe_query_param_schema_is_forwarded(self):
        """schema in query must still be accepted."""
        dialect = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="my-account",
            query={"schema": "MY_SCHEMA"},
        )
        _, opts = dialect.create_connect_args(url)
        assert opts.get("schema") == "MY_SCHEMA"


class TestBlockedKwargsIntegrity:
    """Guard the block-list and document why an allowlist can't replace it.

    Every restricted name is a *valid* connector kwarg (present in the connector's
    DEFAULT_CONFIGURATION).  That is exactly why an allowlist built from
    DEFAULT_CONFIGURATION would not help — it would admit all of these
    parameters.  The denylist must therefore be explicit, and these tests keep it
    anchored to real connector kwargs so it cannot silently drift.
    """

    def test_every_blocked_kwarg_is_a_real_connector_kwarg(self):
        """Each blocked name must exist in the connector's DEFAULT_CONFIGURATION.

        Doubles as drift protection: if a future connector release renames or
        removes one of these kwargs, this test fails and prompts a review of the
        block-list rather than letting it quietly point at a non-existent name.

        Kwargs listed in ``_BLOCKED_KWARGS_4X_ONLY`` were added in connector 4.x
        and are legitimately absent when running against 3.x; they are excluded
        from the hard assertion so the suite passes on both versions.
        """
        connector_keys = set(DEFAULT_CONFIGURATION)
        missing = sorted(_URL_QUERY_BLOCKED_KWARGS - connector_keys)
        # Exclude version-specific kwargs (absent in 3.x, present in 4.x+).
        unexpected_missing = sorted(set(missing) - _BLOCKED_KWARGS_4X_ONLY)
        assert not unexpected_missing, (
            "blocked kwargs missing from connector DEFAULT_CONFIGURATION "
            f"(renamed/removed upstream?): {unexpected_missing}. "
            "This also confirms an allowlist of DEFAULT_CONFIGURATION would still "
            "admit the remaining blocked params."
        )

    def test_blocklist_is_a_nonempty_frozenset(self):
        """Sanity: the guard set is immutable and not accidentally emptied."""
        assert isinstance(_URL_QUERY_BLOCKED_KWARGS, frozenset)
        assert _URL_QUERY_BLOCKED_KWARGS

    @pytest.mark.parametrize(
        "safe_param",
        ["account", "warehouse", "role", "database", "schema", "authenticator"],
    )
    def test_common_safe_params_are_not_over_blocked(self, safe_param):
        """Routine connection params must never be swept into the block-list."""
        assert safe_param not in _URL_QUERY_BLOCKED_KWARGS


class TestConnectArgsMigration:
    """Verify that the new recommended pattern works end-to-end.

    Blocked parameters must never be placed in the URL query string; they
    belong in connect_args= passed to create_engine().  These tests confirm
    that the safe path is fully functional.
    """

    @pytest.fixture(autouse=True)
    def _unset_legacy_flag(self, monkeypatch):
        """Run every test in this class with the secure (non-legacy) code path."""
        monkeypatch.delenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, raising=False)

    def test_url_with_no_blocked_params_is_accepted(self):
        """A URL carrying only safe query params must be accepted without error."""
        d = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="user",
            password="pass",
            host="myaccount",
            query={"warehouse": "COMPUTE_WH", "role": "ANALYST", "timezone": "UTC"},
        )
        _, opts = d.create_connect_args(url)
        assert opts["warehouse"] == "COMPUTE_WH"
        assert opts["role"] == "ANALYST"
        assert opts["timezone"] == "UTC"

    def test_clean_account_and_user_produce_valid_url(self):
        """URL() with plain account/user must produce a well-formed connection string."""
        result = URL(account="xy12345.us-east-1", user="svc_user", password="secret")
        parsed = make_url(result)
        assert parsed.host == "xy12345.us-east-1"
        assert parsed.username == "svc_user"

    def test_user_with_at_sign_roundtrips_via_percent_encoding(self):
        """user='alice@example.com' must be percent-encoded in the URL but the
        connector receives the original plain value after SQLAlchemy decodes it."""
        result = URL(account="myaccount", user="alice@example.com", password="pw")
        from urllib.parse import urlsplit

        netloc = urlsplit(result).netloc
        host_part = netloc.split("@")[-1]
        assert (
            "example.com" not in host_part
        ), f"@ in user leaked into host; netloc={netloc!r}"
        # The encoded form must be present so SQLAlchemy can decode it back
        assert "alice" in result

    def test_user_with_special_chars_does_not_corrupt_url(self):
        """user containing '?', '#', ':' must be encoded, not treated as URL syntax."""
        from urllib.parse import urlsplit

        result = URL(account="acct", user="u?x=1#frag:pw", password="pw")
        raw = urlsplit(result)
        assert raw.query == "" or "x=1" not in raw.query
        assert raw.fragment == ""

    def test_protocol_absent_from_url_query_when_passed_via_connect_args(self):
        """Passing protocol via connect_args= must not appear in the URL query string
        and must not trigger any error from create_connect_args."""
        d = _dialect()
        # Simulate create_engine("snowflake://…", connect_args={"protocol": "https"})
        # connect_args are applied by SQLAlchemy after create_connect_args; the dialect
        # never sees them, so the URL must simply contain no blocked params.
        url = SAUrl.create(
            "snowflake",
            username="user",
            password="pass",
            host="myaccount",
            query={"warehouse": "WH"},
        )
        _, opts = d.create_connect_args(url)
        assert "protocol" not in opts  # not in URL query string
        assert opts["warehouse"] == "WH"


class TestLegacyURLParamsMode:
    """Verify the legacy compatibility shim, enabled via the ``legacy_url_params``
    engine kwarg or the SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS env variable.

    With the shim active, blocked params must emit a DeprecationWarning and
    still be forwarded to the connector — preserving backwards compatibility for
    applications that have not yet migrated to connect_args=.
    """

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        """Start each test with the env variable unset; tests opt in explicitly."""
        monkeypatch.delenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, raising=False)

    @pytest.fixture(params=["kwarg", "env"])
    def legacy_dialect(self, request, monkeypatch):
        """A dialect with the shim enabled — once via kwarg, once via env var."""
        if request.param == "env":
            monkeypatch.setenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, "1")
            return _dialect()
        return base.dialect(legacy_url_params=True)

    @pytest.mark.parametrize("param,value", _ALL_BLOCKED_PARAMS)
    def test_blocked_param_warns_and_is_forwarded(self, legacy_dialect, param, value):
        """Each blocked param must warn and be forwarded — for both enable sources."""
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={param: value},
        )
        with pytest.warns(DeprecationWarning, match=re.escape(param)):
            _, opts = legacy_dialect.create_connect_args(url)
        assert (
            opts.get(param) is not None
        ), f"legacy mode: {param!r} was not forwarded to opts"

    def test_kwarg_enables_shim_without_env(self):
        """legacy_url_params=True must enable the shim with no env variable set."""
        d = base.dialect(legacy_url_params=True)
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={"protocol": "https"},
        )
        with pytest.warns(DeprecationWarning):
            d.create_connect_args(url)

    def test_explicit_kwarg_false_overrides_env_fallback(self, monkeypatch):
        """An explicit legacy_url_params=False wins over the env variable fallback."""
        monkeypatch.setenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, "1")
        d = base.dialect(legacy_url_params=False)
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={"protocol": "https"},
        )
        with pytest.raises(exc.ArgumentError, match="(?i)protocol"):
            d.create_connect_args(url)

    def test_legacy_url_params_is_not_honoured_as_url_query_param(self, monkeypatch):
        """The shim must not be self-enabled via the URL query string.

        ?legacy_url_params=true in the query string must NOT relax the handling —
        otherwise it would be trivially avoidable by whoever controls the URL.
        """
        monkeypatch.delenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, raising=False)
        d = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={"legacy_url_params": "true", "protocol": "https"},
        )
        with pytest.raises(exc.ArgumentError, match="(?i)protocol"):
            d.create_connect_args(url)

    @pytest.mark.parametrize("flag_value", ["1", "true", "True", "TRUE"])
    def test_env_flag_is_accepted_case_insensitively(self, flag_value, monkeypatch):
        """The env variable must be recognised for all parse_url_boolean truthy values."""
        monkeypatch.setenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, flag_value)
        d = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={"protocol": "https"},
        )
        with pytest.warns(DeprecationWarning):
            d.create_connect_args(url)  # must not raise

    @pytest.mark.parametrize("flag_value", ["0", "false", "no", "", "garbage"])
    def test_non_truthy_env_values_keep_blocking(self, flag_value, monkeypatch):
        """Falsy / unrecognised env values must leave the secure block in place."""
        monkeypatch.setenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, flag_value)
        d = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={"protocol": "https"},
        )
        with pytest.raises(exc.ArgumentError, match="(?i)protocol"):
            d.create_connect_args(url)

    def test_neither_source_set_blocks_param(self):
        """With neither kwarg nor env set, the param must be rejected."""
        d = _dialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="legit-account",
            query={"protocol": "https"},
        )
        with pytest.raises(exc.ArgumentError, match="(?i)protocol"):
            d.create_connect_args(url)

    def test_account_with_metachar_warns_in_legacy_mode(self, monkeypatch):
        """_validate_url_field must warn (not raise) for an account containing URL
        metacharacters when the env variable is set.  URL() is a standalone builder
        with no engine, so only the env variable (not the kwarg) can relax it at
        build time."""
        monkeypatch.setenv(SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS, "1")
        with pytest.warns(DeprecationWarning):
            URL(account="x?extra=1", user="u", password="pw")


class TestCacheColumnMetadataKwargPrecedence:
    """create_connect_args must use the preserve-kwarg idiom for
    cache_column_metadata, matching enable_decfloat / case_sensitive_identifiers."""

    def _url(self, **query):
        return SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query=query,
        )

    def test_constructor_true_survives_url_without_param(self):
        """cache_column_metadata=True in the constructor must NOT be reset to
        False when the URL query string omits the param."""
        d = base.dialect(cache_column_metadata=True)
        d.create_connect_args(self._url())
        assert d._cache_column_metadata is True

    def test_constructor_default_false_when_url_silent(self):
        """Default stays False when neither constructor nor URL sets it."""
        d = base.dialect()
        d.create_connect_args(self._url())
        assert d._cache_column_metadata is False

    @pytest.mark.parametrize(
        "url_value, expected",
        [("True", True), ("False", False)],
    )
    def test_url_value_overrides_constructor(self, url_value, expected):
        """An explicit URL value still wins over the constructor kwarg."""
        d = base.dialect(cache_column_metadata=not expected)
        d.create_connect_args(self._url(cache_column_metadata=url_value))
        assert d._cache_column_metadata is expected

    def test_url_param_not_forwarded_to_connector(self):
        """cache_column_metadata is consumed by the dialect, never forwarded."""
        d = base.dialect()
        _, opts = d.create_connect_args(self._url(cache_column_metadata="True"))
        assert "cache_column_metadata" not in opts
