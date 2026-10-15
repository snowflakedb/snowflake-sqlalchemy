#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Tests for strict host-name mode (SNOW-4248737).
#
# Covers:
#   - Strict-mode pass-through (connect() forwards host/auth unvalidated)
#   - Named-profile pass-through (connection_name reaches connector unchanged)
#   - Profile-only URL syntax: URL(account="")
#   - Non-strict backwards compatibility (bare account → .snowflakecomputing.com)
#   - Azure endpoint parameterization (from_uri, constructor, compilation)
#   - Strict Azure compilation guard (missing endpoint fails at compile time)
#

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import exc
from sqlalchemy.engine.url import URL as SAUrl

from snowflake.sqlalchemy import URL, base
from snowflake.sqlalchemy.custom_commands import (
    AzureContainer,
    CreateStage,
    ExternalStage,
)
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strict_dialect():
    return SnowflakeDialect(strict_host_names=True)


def _non_strict_dialect():
    return SnowflakeDialect(strict_host_names=False)


# ---------------------------------------------------------------------------
# Strict-mode create_connect_args (no .snowflakecomputing.com appended)
# ---------------------------------------------------------------------------


class TestStrictModeCreateConnectArgs:
    def test_strict_mode_does_not_append_suffix(self):
        """POC-STRICT-03: strict mode never emits .snowflakecomputing.com."""
        d = _strict_dialect()
        url = SAUrl.create(
            "snowflake",
            username="user",
            password="pass",
            host="org-acct.db.example.com",
        )
        _, opts = d.create_connect_args(url)
        assert opts["host"] == "org-acct.db.example.com"
        assert ".snowflakecomputing.com" not in opts["host"]

    def test_non_strict_preserves_normalization(self):
        """POC-STRICT-07: non-strict bare-account behaviour unchanged."""
        d = _non_strict_dialect()
        url = SAUrl.create(
            "snowflake",
            username="user",
            password="pass",
            host="myaccount",
        )
        _, opts = d.create_connect_args(url)
        assert opts["host"] == "myaccount.snowflakecomputing.com"


# ---------------------------------------------------------------------------
# Strict-mode connect() pass-through
# ---------------------------------------------------------------------------


class TestStrictModeConnect:
    """connect() never validates host, port or auth; the connector owns them."""

    def _connect_kwargs(self, **cparams):
        d = _strict_dialect()
        mock_dbapi = MagicMock()
        type(d).loaded_dbapi = property(lambda self: mock_dbapi)
        try:
            d.connect(**cparams)
            return mock_dbapi.connect.call_args[1]
        finally:
            if "loaded_dbapi" in type(d).__dict__:
                del type(d).loaded_dbapi

    @pytest.mark.parametrize(
        "host",
        [
            pytest.param("org-acct.db.example.com", id="fqdn"),
            pytest.param("myorg-my_account.db.example.com", id="underscore"),
            pytest.param("myaccount", id="bare_account"),
        ],
    )
    def test_host_forwarded_unchanged(self, host):
        call_kwargs = self._connect_kwargs(
            host=host, user="u", password="p", account="acct"
        )
        assert call_kwargs["host"] == host

    def test_missing_host_forwarded_to_connector(self):
        call_kwargs = self._connect_kwargs(user="u", password="p", account="acct")
        assert "host" not in call_kwargs
        assert call_kwargs["account"] == "acct"


# ---------------------------------------------------------------------------
# Named-profile mode (connection_name pass-through)
# ---------------------------------------------------------------------------


class TestNamedProfileMode:
    def _make_dialect_with_mock_dbapi(self):
        d = _strict_dialect()
        mock_dbapi = MagicMock()
        mock_conn = MagicMock()
        mock_dbapi.connect.return_value = mock_conn
        type(d).loaded_dbapi = property(lambda self: mock_dbapi)
        return d, mock_dbapi

    def test_connection_name_passthrough(self):
        """POC-CONFIG-01/03: connection_name reaches connector unchanged."""
        d, mock_dbapi = self._make_dialect_with_mock_dbapi()
        try:
            d.connect(connection_name="my_profile")
            call_kwargs = mock_dbapi.connect.call_args[1]
            assert call_kwargs["connection_name"] == "my_profile"
        finally:
            if "loaded_dbapi" in type(d).__dict__:
                del type(d).loaded_dbapi

    def test_profile_mode_strips_empty_host(self):
        """POC-CONFIG-07: empty host/account stripped when connection_name set."""
        d, mock_dbapi = self._make_dialect_with_mock_dbapi()
        try:
            d.connect(connection_name="my_profile", host="", account="")
            call_kwargs = mock_dbapi.connect.call_args[1]
            assert "host" not in call_kwargs
            assert "account" not in call_kwargs
            assert call_kwargs["connection_name"] == "my_profile"
        finally:
            if "loaded_dbapi" in type(d).__dict__:
                del type(d).loaded_dbapi


# ---------------------------------------------------------------------------
# URL(account="") profile-only syntax
# ---------------------------------------------------------------------------


class TestProfileOnlyURL:
    def test_empty_account_produces_valid_url(self):
        """POC-CONFIG-01: URL(account='') produces a parseable URL string."""
        result = URL(account="")
        assert "snowflake://" in result

    def test_empty_account_create_connect_args(self):
        """URL(account='') doesn't crash in create_connect_args."""
        from sqlalchemy.engine.url import make_url

        d = _strict_dialect()
        url = make_url(URL(account=""))
        _, opts = d.create_connect_args(url)
        # The host should be empty or absent; no .snowflakecomputing.com
        host = opts.get("host", "")
        assert ".snowflakecomputing.com" not in host


# ---------------------------------------------------------------------------
# AzureContainer endpoint parameterization
# ---------------------------------------------------------------------------


class TestAzureContainerEndpoint:
    def test_from_uri_commercial(self):
        """POC-AZURE-04: standard commercial URI still works."""
        c = AzureContainer.from_uri(
            "azure://myaccount.blob.core.windows.net/mycontainer"
        )
        assert c.account == "myaccount"
        assert c.container == "mycontainer"
        assert c.endpoint == "blob.core.windows.net"

    def test_from_uri_custom_endpoint(self):
        """POC-AZURE-01: non-commercial Azure authority preserved."""
        c = AzureContainer.from_uri(
            "azure://myaccount.blob.storage.example.com/mycontainer"
        )
        assert c.account == "myaccount"
        assert c.container == "mycontainer"
        assert c.endpoint == "blob.storage.example.com"

    def test_from_uri_with_path(self):
        c = AzureContainer.from_uri(
            "azure://myaccount.blob.storage.example.com/mycontainer/path/to/data"
        )
        assert c.account == "myaccount"
        assert c.container == "mycontainer"
        assert c.path == "path/to/data"
        assert c.endpoint == "blob.storage.example.com"

    def test_constructor_explicit_endpoint(self):
        """POC-AZURE-02: explicit endpoint rendered exactly once."""
        c = AzureContainer(
            account="myaccount",
            container="mycontainer",
            endpoint="blob.storage.example.com",
        )
        repr_str = repr(c)
        assert "blob.storage.example.com" in repr_str
        assert repr_str.count("blob.storage.example.com") == 1
        assert "blob.core.windows.net" not in repr_str

    def test_constructor_no_endpoint_uses_commercial_default(self):
        """POC-AZURE-04: no endpoint → commercial default in non-strict."""
        c = AzureContainer(account="myaccount", container="mycontainer")
        repr_str = repr(c)
        assert "blob.core.windows.net" in repr_str

    def test_invalid_uri_rejected(self):
        with pytest.raises(ValueError, match="Invalid Azure Container URI"):
            AzureContainer.from_uri("s3://wrong-scheme/bucket")


# ---------------------------------------------------------------------------
# Azure strict-mode compilation guard
# ---------------------------------------------------------------------------


class TestAzureStrictCompilation:
    def test_strict_rejects_missing_endpoint(self):
        """POC-AZURE-03: strict compilation fails when no explicit endpoint."""
        from sqlalchemy import literal_column, select

        d = _strict_dialect()
        container = AzureContainer(account="myaccount", container="mycontainer")

        compiler = base.SnowflakeCompiler(d, select(literal_column("1")), {})
        with pytest.raises(exc.CompileError, match="explicit Azure Blob endpoint"):
            compiler.visit_azure_container(container)

    def test_strict_accepts_explicit_endpoint(self):
        """POC-AZURE-02: explicit endpoint compiles without error in strict mode."""
        from sqlalchemy import literal_column, select

        d = _strict_dialect()
        container = AzureContainer(
            account="myaccount",
            container="mycontainer",
            endpoint="blob.storage.example.com",
        )

        compiler = base.SnowflakeCompiler(d, select(literal_column("1")), {})
        uri, _, _ = compiler.visit_azure_container(container)
        assert "blob.storage.example.com" in uri
        assert "blob.core.windows.net" not in uri

    def test_non_strict_allows_missing_endpoint(self):
        """POC-AZURE-04: non-strict compiles with commercial default."""
        from sqlalchemy import literal_column, select

        d = _non_strict_dialect()
        container = AzureContainer(account="myaccount", container="mycontainer")

        compiler = base.SnowflakeCompiler(d, select(literal_column("1")), {})
        uri, _, _ = compiler.visit_azure_container(container)
        assert "blob.core.windows.net" in uri


class TestAzureStrictCompilationEdgeCases:
    def test_strict_rejects_empty_string_endpoint(self):
        """Empty-string endpoint must not bypass the strict guard."""
        from sqlalchemy import literal_column, select

        d = _strict_dialect()
        container = AzureContainer(
            account="myaccount", container="mycontainer", endpoint=""
        )

        compiler = base.SnowflakeCompiler(d, select(literal_column("1")), {})
        with pytest.raises(exc.CompileError, match="explicit Azure Blob endpoint"):
            compiler.visit_azure_container(container)

    def test_strict_create_stage_rejects_missing_endpoint(self):
        """CreateStage must also enforce the strict Azure endpoint guard."""
        d = _strict_dialect()
        stage = ExternalStage(name="S", namespace="DB.SCH")
        container = AzureContainer(account="myaccount", container="mycontainer")
        create_stage = CreateStage(stage=stage, container=container)

        with pytest.raises(exc.CompileError, match="explicit Azure Blob endpoint"):
            create_stage.compile(
                dialect=d,
                compile_kwargs={"literal_binds": True, "deterministic": True},
            )

    def test_non_strict_create_stage_uses_commercial_default(self):
        """Non-strict CreateStage still renders .blob.core.windows.net."""
        d = _non_strict_dialect()
        stage = ExternalStage(name="S", namespace="DB.SCH")
        container = AzureContainer(account="myaccount", container="mycontainer")
        create_stage = CreateStage(stage=stage, container=container)

        sql = str(
            create_stage.compile(
                dialect=d,
                compile_kwargs={"literal_binds": True, "deterministic": True},
            )
        )
        assert "blob.core.windows.net" in sql


# ---------------------------------------------------------------------------
# Strict-mode account derivation
# ---------------------------------------------------------------------------


class TestStrictModeAccountDerivation:
    def test_strict_mode_still_sets_account(self):
        """Strict mode must derive account from URL host even without normalization."""
        d = _strict_dialect()
        url = SAUrl.create(
            "snowflake",
            username="user",
            password="pass",
            host="org-acct.db.example.com",
        )
        _, opts = d.create_connect_args(url)
        assert "account" in opts
        assert opts["account"]  # must be non-empty


# ---------------------------------------------------------------------------
# Environment variable activation
# ---------------------------------------------------------------------------


class TestEnvVarActivation:
    def test_env_var_enables_strict_mode(self, monkeypatch):
        """POC-STRICT-01: env var activates strict mode."""
        monkeypatch.setenv("SNOWFLAKE_SQLALCHEMY_STRICT_HOST_NAMES", "true")
        d = SnowflakeDialect()
        assert d._strict_host_names is True

    def test_env_var_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_SQLALCHEMY_STRICT_HOST_NAMES", raising=False)
        d = SnowflakeDialect()
        assert d._strict_host_names is False

    def test_kwarg_overrides_env_var(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_SQLALCHEMY_STRICT_HOST_NAMES", "true")
        d = SnowflakeDialect(strict_host_names=False)
        assert d._strict_host_names is False
