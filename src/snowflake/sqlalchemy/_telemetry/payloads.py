#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Connector-agnostic telemetry payload building (the "gather" layer).

These functions build the telemetry payloads from the SQLAlchemy dialect and
the connection parameters.  They are deliberately independent of the connector
telemetry API (no ``snowflake.connector.telemetry`` imports), so the "register"
step is decoupled from the version-specific "send" step.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import __version__ as SQLALCHEMY_VERSION

from .._constants import (
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
)

if TYPE_CHECKING:
    from ..snowdialect import SnowflakeDialect

# ---------------------------------------------------------------------------
# Connection-parameter telemetry
# ---------------------------------------------------------------------------
# Connector connection kwargs whose *values* are safe (non-PII, low
# cardinality) to record on the NEW_CONNECTION_PARAMETERS telemetry event.
# Anything not listed here is recorded by key-presence only (see
# ``build_connection_parameters_payload``); identifier-like params
# (warehouse/role/database/schema) are recorded by key only, and every
# credential / authentication-method param (see ``_TELEMETRY_CREDENTIAL_PARAMS``)
# is dropped entirely so neither its value nor its key name leaves the client.
_TELEMETRY_SAFE_VALUE_PARAMS = frozenset(
    {
        "paramstyle",
        "numpy",
        "client_session_keep_alive",
        "protocol",
        "ocsp_fail_open",
        "validate_default_parameters",
        "client_prefetch_threads",
        "login_timeout",
        "network_timeout",
        "autocommit",
        "arrow_number_to_decimal",
        "client_store_temporary_credential",
        "disable_request_pooling",
    }
)

# Credential and authentication-method connection kwargs.  These are excluded
# from ``provided_keys`` entirely (CWE-532): even the *presence* of a key name
# like ``password``/``private_key``/``oauth_token`` reveals which authentication
# method a customer uses.  That is already derivable server-side from the
# connector's login request (``ACCOUNT_USAGE.SESSIONS.AUTHENTICATION_METHOD`` /
# ``LOGIN_HISTORY``) via the ``SnowflakeSQLAlchemy`` app identity, so recording
# it here would only duplicate server-known data while narrowing the customer's
# described attack surface.  Names track ``snowflake.connector`` connection
# kwargs; add new credential/auth params here as the connector gains them.
_TELEMETRY_CREDENTIAL_PARAMS = frozenset(
    {
        # secrets
        "password",
        "password_callback",
        "proxy_password",
        "passcode",
        "token",
        "token_file_path",
        "master_token",
        "session_token",
        "private_key",
        "private_key_file",
        "private_key_file_pwd",
        "private_key_passphrase",
        "oauth_client_secret",
        # authentication-method selectors / configuration
        "authenticator",
        "auth_class",
        "passcode_in_password",
        "client_request_mfa_token",
        "consent_cache_id_token",
        "oauth_client_id",
        "oauth_authorization_url",
        "oauth_token_request_url",
        "oauth_redirect_uri",
        "oauth_scope",
        "oauth_socket_uri",
        "oauth_credentials_in_body",
        "oauth_disable_pkce",
        "oauth_enable_refresh_tokens",
        "oauth_enable_single_use_refresh_tokens",
        "workload_identity_provider",
        "workload_identity_entra_resource",
        "workload_identity_impersonation_path",
    }
)

# Connection kwargs the dialect injects itself (see
# ``_update_connection_application_name``).  Excluded from the recorded key set
# so telemetry reflects the customer's own choices, not our defaults.
_TELEMETRY_INJECTED_PARAMS = frozenset(
    {
        PARAM_APPLICATION,
        PARAM_INTERNAL_APPLICATION_NAME,
        PARAM_INTERNAL_APPLICATION_VERSION,
    }
)


def _telemetry_safe_value(name: str, value: Any) -> Any:
    """Coerce an allow-listed param value to a JSON-friendly primitive."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def build_new_connection_payload(dialect: SnowflakeDialect) -> dict[str, Any]:
    """Build the legacy flat NEW_CONNECTION payload (sent as ``str(dict)``).

    Records the SQLAlchemy/pandas versions plus the dialect-level configuration
    flags.  These are user-chosen booleans that do not contain PII but
    meaningfully change how the dialect normalises identifiers, generates SQL,
    and reflects schemas.  Values are read from ``dialect`` so they reflect the
    final post-plugin / post-URL state regardless of how they were configured.
    """
    telemetry_value: dict[str, Any] = {"SQLAlchemy": SQLALCHEMY_VERSION}
    try:
        from pandas import __version__ as PANDAS_VERSION

        telemetry_value["pandas"] = PANDAS_VERSION
    except ImportError:
        pass

    telemetry_value["case_sensitive_identifiers"] = dialect._case_sensitive_identifiers
    telemetry_value["enable_decfloat"] = dialect._enable_decfloat
    telemetry_value["enable_structured_type_json"] = (
        dialect._enable_structured_type_json
    )
    telemetry_value["force_div_is_floordiv"] = dialect.force_div_is_floordiv
    telemetry_value["legacy_url_params"] = dialect._legacy_url_params
    return telemetry_value


def build_connection_parameters_payload(
    dialect: SnowflakeDialect, cparams: dict | None
) -> dict[str, Any]:
    """Build the structured, queryable NEW_CONNECTION_PARAMETERS payload.

    PII-safe: credential and identifier *values* are never included.  Every
    supplied option is recorded by key-presence only (``provided_keys``),
    except credential / authentication-method params
    (``_TELEMETRY_CREDENTIAL_PARAMS``), which are dropped entirely so neither
    their value nor their key name is recorded -- the auth method is derivable
    server-side from the connector's login request.  Values are copied solely
    for the curated ``_TELEMETRY_SAFE_VALUE_PARAMS`` allow-list.
    """
    cparams = cparams or {}

    flags = {
        "case_sensitive_identifiers": dialect._case_sensitive_identifiers,
        "enable_decfloat": dialect._enable_decfloat,
        "enable_structured_type_json": dialect._enable_structured_type_json,
        # ``cache_column_metadata`` is a connection parameter (not a dialect
        # attribute), so read it from ``cparams`` rather than ``dialect``.
        "cache_column_metadata": bool(cparams.get("cache_column_metadata", False)),
        "force_div_is_floordiv": dialect.force_div_is_floordiv,
        "legacy_url_params": dialect._legacy_url_params,
        "isolation_level": getattr(dialect, "_isolation_level", None),
    }

    versions = {"sqlalchemy": SQLALCHEMY_VERSION}
    try:
        from pandas import __version__ as PANDAS_VERSION

        versions["pandas"] = PANDAS_VERSION
    except ImportError:
        pass

    provided_keys = sorted(
        k
        for k in cparams
        if k not in _TELEMETRY_INJECTED_PARAMS and k not in _TELEMETRY_CREDENTIAL_PARAMS
    )
    values = {
        name: _telemetry_safe_value(name, cparams[name])
        for name in _TELEMETRY_SAFE_VALUE_PARAMS
        if name in cparams
    }

    return {
        "versions": versions,
        "dialect_flags": flags,
        "connection_parameters": {
            "provided_keys": provided_keys,
            "values": values,
        },
    }
