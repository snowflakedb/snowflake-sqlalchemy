#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Opt-in redaction of cloud-storage secrets in log output.

A ``COPY INTO`` / ``CREATE STAGE`` statement that uses inline credentials
(``AWSBucket.credentials(...)``, ``AzureContainer.credentials(...)``,
``*.encryption_*_cse(master_key)``) necessarily carries those secrets as literal
values in the compiled SQL — that is how Snowflake receives them.  When the
SQLAlchemy engine logger is enabled (``create_engine(..., echo=True)`` or the
``sqlalchemy.engine`` logger at INFO/DEBUG) the full statement, secrets included,
is emitted verbatim; that logger is not routed through the Snowflake connector's
``SecretDetector`` (SNOW-3649850).

The robust way to avoid the secret reaching logs at all is to use a named
``STORAGE_INTEGRATION`` instead of inline credentials, so no secret ever appears
in the SQL.  When inline credentials are unavoidable, attach
:class:`SnowflakeSecretRedactionFilter` to the handler (or logger) that emits the
statements; it masks the secret values while leaving the rest of the statement
intact.
"""

import logging
import re

from .custom_commands import REDACTED_SECRET, SECRET_OPTION_KEYS

__all__ = [
    "redact_secrets",
    "SnowflakeSecretRedactionFilter",
    "add_secret_redaction_filter",
]

# Match ``KEY='...'`` (allowing whitespace around ``=``) for any secret option
# key.  The literal body tolerates doubled single quotes (``''``) and backslash
# escapes, matching the Snowflake string-literal escaping the dialect emits, so
# an escaped quote inside the secret does not end the match early.
_SECRET_LITERAL_RE = re.compile(
    r"(?P<key>(?:%s))(?P<sep>\s*=\s*)'(?:''|\\.|[^'\\])*'"
    % "|".join(re.escape(k) for k in sorted(SECRET_OPTION_KEYS))
)


def redact_secrets(text: str) -> str:
    """Replace secret option literals in ``text`` with ``KEY='***'``.

    Only the values of :data:`SECRET_OPTION_KEYS` are masked; structural options
    (``TYPE``, ``AWS_ROLE``, ``KMS_KEY_ID``, ...) and the rest of the statement
    are left untouched.  Safe to call on any string; non-matching text is
    returned unchanged.
    """
    return _SECRET_LITERAL_RE.sub(
        lambda m: f"{m.group('key')}{m.group('sep')}'{REDACTED_SECRET}'", text
    )


class SnowflakeSecretRedactionFilter(logging.Filter):
    """Logging filter that redacts cloud-storage secrets from log records.

    Attach to the handler (preferred) or logger that emits SQLAlchemy engine
    statements.  Never drops records (always returns ``True``); it only rewrites
    the message and string arguments in place.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_secrets(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: (redact_secrets(v) if isinstance(v, str) else v)
                    for k, v in record.args.items()
                }
            else:
                record.args = tuple(
                    redact_secrets(a) if isinstance(a, str) else a
                    for a in record.args
                )
        return True


def add_secret_redaction_filter(target):
    """Attach a :class:`SnowflakeSecretRedactionFilter` to ``target``.

    ``target`` may be a :class:`logging.Logger` or a :class:`logging.Handler`.
    Attaching to the handler is the reliable choice: filters on an ancestor
    logger are not re-applied to records that merely propagate up to it, whereas
    handler filters run on every record the handler emits.  Returns the filter
    instance so it can later be removed with ``target.removeFilter(...)``.
    """
    if not isinstance(target, (logging.Logger, logging.Handler)):
        raise TypeError(
            "target must be a logging.Logger or logging.Handler, "
            f"got {type(target).__name__}"
        )
    redaction_filter = SnowflakeSecretRedactionFilter()
    target.addFilter(redaction_filter)
    return redaction_filter
