#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import false, true
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.roles import FromClauseRole
from sqlalchemy.sql.selectable import FromClause, Selectable

from .util import escape_single_quotes, escape_string_literal_interior

NoneType = type(None)

# Cloud-storage option keys whose values are bearer secrets (cloud access keys,
# SAS tokens, client-side-encryption master keys).  These must never appear in
# any debug/log representation (SNOW-3649782 / SNOW-3649850).  Structural option
# keys (TYPE, AWS_ROLE, KMS_KEY_ID, ...) are not secrets and stay visible.
SECRET_OPTION_KEYS = frozenset(
    {"AWS_SECRET_KEY", "AWS_KEY_ID", "AWS_TOKEN", "AZURE_SAS_TOKEN", "MASTER_KEY"}
)
REDACTED_SECRET = "***"


def _redact_option(name, value):
    """Return ``value`` for non-secret option keys, ``***`` for secret ones."""
    return REDACTED_SECRET if name in SECRET_OPTION_KEYS else value


# FILE_FORMAT option keys whose values are free-form text with no Snowflake
# backslash-escape semantics.  These receive full escaping (doubles both ' and
# \).  All other string options use quote-only escaping (' → ''), preserving
# legitimate backslash sequences: delimiter/escape options validated to a single
# character by _check_delimiter (e.g. RECORD_DELIMITER='\n'), and NULL_IF
# elements which may carry the Snowflake null token \N (SNOW-3649888).
_FULL_ESCAPE_OPTION_KEYS = frozenset(
    {
        "COMPRESSION",
        "DATE_FORMAT",
        "FILE_EXTENSION",
        "TIME_FORMAT",
        "TIMESTAMP_FORMAT",
    }
)


def translate_bool(bln: bool) -> ClauseElement:
    if bln:
        return true()
    return false()


class MergeInto(UpdateBase):
    __visit_name__ = "merge_into"
    _bind = None

    def __init__(
        self, target: FromClause, source: FromClause | Selectable, on: ClauseElement
    ) -> None:
        self.target = target
        self.source = source
        self.on = on
        self.clauses: list[MergeInto.clause] = []

    class clause(ClauseElement):
        __visit_name__ = "merge_into_clause"

        def __init__(self, command: str) -> None:
            self.set: dict[str, Any] = {}
            self.predicate: ClauseElement | None = None
            self.command = command

        def __repr__(self) -> str:
            case_predicate = (
                f" AND {str(self.predicate)}" if self.predicate is not None else ""
            )
            if self.command == "INSERT":
                sets: Any
                sets, sets_tos = zip(*self.set.items(), strict=True)
                return "WHEN NOT MATCHED{} THEN {} ({}) VALUES ({})".format(
                    case_predicate,
                    self.command,
                    ", ".join(sets),
                    ", ".join(map(str, sets_tos)),
                )
            else:
                # WHEN MATCHED clause
                sets = (
                    ", ".join([f"{set[0]} = {set[1]}" for set in self.set.items()])
                    if self.set
                    else ""
                )
                return "WHEN MATCHED{} THEN {}{}".format(
                    case_predicate,
                    self.command,
                    f" SET {str(sets)}" if self.set else "",
                )

        def values(self, **kwargs: Any) -> MergeInto.clause:
            self.set = kwargs
            return self

        def where(self, expr: ClauseElement) -> MergeInto.clause:
            self.predicate = expr
            return self

    def __repr__(self) -> str:
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return f"MERGE INTO {self.target} USING {self.source} ON {self.on}" + (
            f" {clauses}" if clauses else ""
        )

    def when_matched_then_update(self) -> MergeInto.clause:
        clause = self.clause("UPDATE")
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self) -> MergeInto.clause:
        clause = self.clause("DELETE")
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self) -> MergeInto.clause:
        clause = self.clause("INSERT")
        self.clauses.append(clause)
        return clause


class InsertMulti(UpdateBase):
    """Snowflake multi-table insert (``INSERT ALL`` / ``INSERT FIRST``).

    Build an unconditional insert with :meth:`into`, or a conditional insert with
    :meth:`when` / :meth:`else_`.  Each target may optionally specify ``columns``
    and matching source ``values``; when omitted, the subquery output columns are
    used positionally.

    See https://docs.snowflake.com/en/sql-reference/sql/insert-multi-table
    """

    __visit_name__ = "insert_multi"
    _bind = None
    # The compiled SQL depends on builder state (clauses/else__/overwrite/first/
    # source) that is not covered by any cache-key traversal, so this construct
    # cannot inherit UpdateBase's cache key; opt out of statement caching.
    inherit_cache = False

    def __init__(
        self, source: Any, overwrite: bool = False, first: bool = False
    ) -> None:
        self.source = source
        self.overwrite = overwrite
        self.first = first
        self.clauses: list[tuple[Any, Any, Any, Any]] = []
        self.else__: tuple[Any, Any, Any] | None = None

    @property
    def is_conditional(self) -> bool:
        return any(condition is not None for condition, _, _, _ in self.clauses)

    def __repr__(self) -> str:
        clauses = []
        for condition, table, columns, values in self.clauses:
            clauses.append(
                (f"WHEN {condition!r} THEN " if condition is not None else "")
                + f"INTO {table!r}"
                + (f"({', '.join(repr(c) for c in columns)})" if columns else "")
                + (f" VALUES ({', '.join(str(v) for v in values)})" if values else "")
            )
        else_ = f" ELSE {self.else__!r}" if self.else__ else ""
        overwrite = " OVERWRITE" if self.overwrite else ""
        condition = "FIRST" if self.is_conditional and self.first else "ALL"
        return f"INSERT{overwrite} {condition} {' '.join(clauses)}{else_} {self.source}"

    def _adapt_columns(self, columns: Any, coll: Any) -> Any:
        """Make sure all columns are column instances from the given table, not strings."""
        if columns is None:
            return None
        return [coll[c] if isinstance(c, str) else c for c in columns]

    def into(self, table: Any, columns: Any = None, values: Any = None) -> InsertMulti:
        if self.is_conditional:
            raise ValueError(
                "Cannot add an unconditional clause to a conditional multi-table insert"
            )
        if columns and values:
            assert len(columns) == len(values), (
                "columns and values must be of the same length"
            )
        self.clauses.append(
            (
                None,
                table,
                self._adapt_columns(columns, table.c),
                self._adapt_columns(values, self.source.selected_columns),
            )
        )
        return self

    def when(
        self, condition: Any, table: Any, columns: Any = None, values: Any = None
    ) -> InsertMulti:
        if condition is None:
            raise ValueError(
                "when() requires a non-None condition; use into() for an "
                "unconditional multi-table insert"
            )
        if self.clauses and not self.is_conditional:
            raise ValueError(
                "Cannot add a conditional clause to an unconditional multi-table insert"
            )
        if columns and values:
            assert len(columns) == len(values), (
                "columns and values must be of the same length"
            )
        self.clauses.append(
            (
                condition,
                table,
                self._adapt_columns(columns, table.c),
                self._adapt_columns(values, self.source.selected_columns),
            )
        )
        return self

    def else_(self, table: Any, columns: Any = None, values: Any = None) -> InsertMulti:
        if not self.is_conditional:
            raise ValueError(
                "ELSE requires at least one conditional WHEN clause; add .when(...) "
                "before .else_(...)"
            )
        self.else__ = (
            table,
            self._adapt_columns(columns, table.c),
            self._adapt_columns(values, self.source.selected_columns),
        )
        return self


class FilesOption:
    """
    Class to represent FILES option for the snowflake COPY INTO statement
    """

    def __init__(self, file_names: list[str]) -> None:
        self.file_names = file_names

    def __str__(self) -> str:
        # File names are frequently externally-influenced (uploads, object-store
        # listings, webhook bodies).  Use the shared Snowflake literal escaping
        # (doubles ' and \) instead of the old \' convention, which left a
        # backslash-before-quote escaping under ESCAPE_STRING_LITERALS
        # (SNOW-3649871).
        the_files = [
            "'" + escape_string_literal_interior(f) + "'" for f in self.file_names
        ]
        return f"({','.join(the_files)})"


class CopyInto(UpdateBase):
    """Copy Into Command base class, for documentation see:
    https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html"""

    __visit_name__ = "copy_into"
    _bind = None

    def __init__(
        self,
        from_: Any,
        into: Any,
        partition_by: Any | None = None,
        formatter: CopyFormatter | None = None,
    ) -> None:
        self.from_ = from_
        self.into = into
        self.formatter = formatter
        self.copy_options: dict[str, Any] = {}
        self.partition_by = partition_by

    def __repr__(self) -> str:
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        val = f"COPY INTO {self.into} FROM {repr(self.from_)}"
        if self.partition_by is not None:
            val += f" PARTITION BY {self.partition_by}"

        return val + f" {repr(self.formatter)} ({self.copy_options})"

    def bind(self) -> None:
        return None

    def force(self, force: bool) -> CopyInto:
        if not isinstance(force, bool):
            raise TypeError("Parameter force should be a boolean value")
        self.copy_options.update({"FORCE": translate_bool(force)})
        return self

    def single(self, single_file: bool) -> CopyInto:
        if not isinstance(single_file, bool):
            raise TypeError("Parameter single_file should  be a boolean value")
        self.copy_options.update({"SINGLE": translate_bool(single_file)})
        return self

    def maxfilesize(self, max_size: int) -> CopyInto:
        if not isinstance(max_size, int):
            raise TypeError("Parameter max_size should be an integer value")
        self.copy_options.update({"MAX_FILE_SIZE": max_size})
        return self

    def files(self, file_names: list[str]) -> CopyInto:
        self.copy_options.update({"FILES": FilesOption(file_names)})
        return self

    def pattern(self, pattern: str) -> CopyInto:
        self.copy_options.update({"PATTERN": pattern})
        return self

    def storage_integration(self, integration_name: str) -> CopyInto:
        self.copy_options.update({"STORAGE_INTEGRATION": integration_name})
        return self


class CopyFormatter(ClauseElement):
    """
    Base class for Formatter specifications inside a COPY INTO statement. May also
    be used to create a named format.
    """

    __visit_name__ = "copy_formatter"

    # Set by concrete subclasses (CSVFormatter="csv", JSONFormatter="json", …);
    # declared here so type-checkers know the attribute exists on the base.
    file_format: str

    def __init__(self, format_name: str | None = None) -> None:
        self.options: dict[str, Any] = dict()
        if format_name:
            self.options["format_name"] = format_name

    def __repr__(self) -> str:
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        return f"FILE_FORMAT=({self.options})"

    @staticmethod
    def _escape_option_str(name, value):
        """Escape the interior of a FILE_FORMAT string option value.

        Free-form text options (dates, times, extensions, compression type)
        receive full escaping (doubles both ' and \\).  Delimiter/escape options
        and NULL_IF receive quote-only escaping (' → '') so that legitimate
        Snowflake backslash sequences (\\n, \\134, \\N) are preserved.
        """
        if name in _FULL_ESCAPE_OPTION_KEYS:
            return escape_string_literal_interior(value)
        return escape_single_quotes(value)

    @staticmethod
    def value_repr(name: str, value: Any) -> str:
        """
        Make a SQL-suitable representation of "value". This is called from
        the corresponding visitor function (base.py/visit_copy_formatter())
        - in case of a format name: return it without quotes
        - in case of a string: enclose in quotes with interior escaping
        - in case of a tuple of length 1: enclose the only element in brackets: (value)
            Standard stringification of Python would append a trailing comma: (value,)
            which is not correct in SQL
        - otherwise: just convert to str as is: value
        """
        if name == "format_name":
            return value
        elif isinstance(value, str):
            return f"'{CopyFormatter._escape_option_str(name, value)}'"
        elif isinstance(value, tuple) and len(value) == 1:
            return f"('{CopyFormatter._escape_option_str(name, str(value[0]))}')"
        else:
            return str(value)


class CSVFormatter(CopyFormatter):
    file_format = "csv"

    def compression(self, comp_type: str | None) -> CSVFormatter:
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, str):
            comp_type = comp_type.lower()
        _available_options = [
            "auto",
            "gzip",
            "bz2",
            "brotli",
            "zstd",
            "deflate",
            "raw_deflate",
            None,
        ]
        if comp_type not in _available_options:
            raise TypeError(f"Compression type should be one of : {_available_options}")
        self.options["COMPRESSION"] = comp_type
        return self

    def _check_delimiter(self, delimiter: str | int | None, delimiter_txt: str) -> None:
        """
        Check if a delimiter is either a string of length 1 or an integer. In case of
        a string delimiter, take into account that the actual string may be longer,
        but still evaluate to a single character (like "\\n" or r"\n"
        """
        if isinstance(delimiter, NoneType):
            return
        if isinstance(delimiter, str):
            delimiter_processed = delimiter.encode().decode("unicode_escape")
            if len(delimiter_processed) == 1:
                return
        if isinstance(delimiter, int):
            return
        raise TypeError(
            f"{delimiter_txt} should be a single character, that is either a string, or a number"
        )

    def record_delimiter(self, deli_type: str | int | None) -> CSVFormatter:
        """Character that separates records in an unloaded file."""
        self._check_delimiter(deli_type, "Record delimiter")
        if isinstance(deli_type, int):
            self.options["RECORD_DELIMITER"] = hex(deli_type)
        else:
            self.options["RECORD_DELIMITER"] = deli_type
        return self

    def field_delimiter(self, deli_type: str | int | None) -> CSVFormatter:
        """Character that separates fields in an unloaded file."""
        self._check_delimiter(deli_type, "Field delimiter")
        if isinstance(deli_type, int):
            self.options["FIELD_DELIMITER"] = hex(deli_type)
        else:
            self.options["FIELD_DELIMITER"] = deli_type
        return self

    def file_extension(self, ext: str | None) -> CSVFormatter:
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service.
        """
        if not isinstance(ext, (NoneType, str)):
            raise TypeError("File extension should be a string")
        self.options["FILE_EXTENSION"] = ext
        return self

    def date_format(self, dt_frmt: str) -> CSVFormatter:
        """String that defines the format of date values in the unloaded data files."""
        if not isinstance(dt_frmt, str):
            raise TypeError("Date format should be a string")
        self.options["DATE_FORMAT"] = dt_frmt
        return self

    def time_format(self, tm_frmt: str) -> CSVFormatter:
        """String that defines the format of time values in the unloaded data files."""
        if not isinstance(tm_frmt, str):
            raise TypeError("Time format should be a string")
        self.options["TIME_FORMAT"] = tm_frmt
        return self

    def timestamp_format(self, tmstmp_frmt: str) -> CSVFormatter:
        """String that defines the format of timestamp values in the unloaded data files."""
        if not isinstance(tmstmp_frmt, str):
            raise TypeError("Timestamp format should be a string")
        self.options["TIMESTAMP_FORMAT"] = tmstmp_frmt
        return self

    def binary_format(self, bin_fmt: str) -> CSVFormatter:
        """Character used as the escape character for any field values. The option can be used when unloading data
        from binary columns in a table."""
        if isinstance(bin_fmt, str):
            bin_fmt = bin_fmt.lower()
        _available_options = ["hex", "base64", "utf8"]
        if bin_fmt not in _available_options:
            raise TypeError(f"Binary format should be one of : {_available_options}")
        self.options["BINARY_FORMAT"] = bin_fmt
        return self

    def escape(self, esc: str | int | None) -> CSVFormatter:
        """Character used as the escape character for any field values."""
        self._check_delimiter(esc, "Escape")
        if isinstance(esc, int):
            self.options["ESCAPE"] = hex(esc)
        else:
            self.options["ESCAPE"] = esc
        return self

    def escape_unenclosed_field(self, esc: str | int | None) -> CSVFormatter:
        """Single character string used as the escape character for unenclosed field values only."""
        self._check_delimiter(esc, "Escape unenclosed field")
        if isinstance(esc, int):
            self.options["ESCAPE_UNENCLOSED_FIELD"] = hex(esc)
        else:
            self.options["ESCAPE_UNENCLOSED_FIELD"] = esc
        return self

    def field_optionally_enclosed_by(self, enc: str | None) -> CSVFormatter:
        """Character used to enclose strings. Either None, ', or \"."""
        _available_options = [None, "'", '"']
        if enc not in _available_options:
            raise TypeError(f"Enclosing string should be one of : {_available_options}")
        self.options["FIELD_OPTIONALLY_ENCLOSED_BY"] = enc
        return self

    def null_if(self, null_value: Sequence) -> CSVFormatter:
        """Copying into a table these strings will be replaced by a NULL, while copying out of Snowflake will replace
        NULL values with the first string"""
        if not isinstance(null_value, Sequence):
            raise TypeError("Parameter null_value should be an iterable")
        self.options["NULL_IF"] = tuple(null_value)
        return self

    def skip_header(self, skip_header: int) -> CSVFormatter:
        """
        Number of header rows to be skipped at the beginning of the file
        """
        if not isinstance(skip_header, int):
            raise TypeError("skip_header  should be an int")
        self.options["SKIP_HEADER"] = skip_header
        return self

    def trim_space(self, trim_space: bool) -> CSVFormatter:
        """
        Remove leading or trailing white spaces
        """
        if not isinstance(trim_space, bool):
            raise TypeError("trim_space should be a bool")
        self.options["TRIM_SPACE"] = trim_space
        return self

    def error_on_column_count_mismatch(
        self, error_on_col_count_mismatch: bool
    ) -> CSVFormatter:
        """
        Generate a parsing error if the number of delimited columns (i.e. fields) in
        an input data file does not match the number of columns in the corresponding table.
        """
        if not isinstance(error_on_col_count_mismatch, bool):
            raise TypeError("skip_header  should be a bool")
        self.options["ERROR_ON_COLUMN_COUNT_MISMATCH"] = error_on_col_count_mismatch
        return self


class JSONFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = "json"

    def compression(self, comp_type: str | None) -> JSONFormatter:
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, str):
            comp_type = comp_type.lower()
        _available_options = [
            "auto",
            "gzip",
            "bz2",
            "brotli",
            "zstd",
            "deflate",
            "raw_deflate",
            None,
        ]
        if comp_type not in _available_options:
            raise TypeError(f"Compression type should be one of : {_available_options}")
        self.options["COMPRESSION"] = comp_type
        return self

    def file_extension(self, ext: str | None) -> JSONFormatter:
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service.
        """
        if not isinstance(ext, (NoneType, str)):
            raise TypeError("File extension should be a string")
        self.options["FILE_EXTENSION"] = ext
        return self

    def date_format(self, dt_frmt: str) -> JSONFormatter:
        """String that defines the format of date values in the data files."""
        if not isinstance(dt_frmt, str):
            raise TypeError("Date format should be a string")
        self.options["DATE_FORMAT"] = dt_frmt
        return self

    def time_format(self, tm_frmt: str) -> JSONFormatter:
        """String that defines the format of time values in the data files."""
        if not isinstance(tm_frmt, str):
            raise TypeError("Time format should be a string")
        self.options["TIME_FORMAT"] = tm_frmt
        return self

    def timestamp_format(self, tmstmp_frmt: str) -> JSONFormatter:
        """String that defines the format of timestamp values in the data files."""
        if not isinstance(tmstmp_frmt, str):
            raise TypeError("Timestamp format should be a string")
        self.options["TIMESTAMP_FORMAT"] = tmstmp_frmt
        return self

    def binary_format(self, bin_fmt: str) -> JSONFormatter:
        """Encoding format for binary input or output: one of hex, base64 or utf8."""
        if isinstance(bin_fmt, str):
            bin_fmt = bin_fmt.lower()
        _available_options = ["hex", "base64", "utf8"]
        if bin_fmt not in _available_options:
            raise TypeError(f"Binary format should be one of : {_available_options}")
        self.options["BINARY_FORMAT"] = bin_fmt
        return self

    def trim_space(self, trim_space: bool) -> JSONFormatter:
        """Remove leading and trailing white space from strings."""
        if not isinstance(trim_space, bool):
            raise TypeError("trim_space should be a bool")
        self.options["TRIM_SPACE"] = trim_space
        return self

    def null_if(self, null_value: Sequence) -> JSONFormatter:
        """Strings to convert to and from SQL NULL."""
        if not isinstance(null_value, Sequence):
            raise TypeError("Parameter null_value should be an iterable")
        self.options["NULL_IF"] = tuple(null_value)
        return self

    def enable_octal(self, value: bool) -> JSONFormatter:
        """Enable parsing of octal numbers."""
        if not isinstance(value, bool):
            raise TypeError("enable_octal should be a bool")
        self.options["ENABLE_OCTAL"] = value
        return self

    def allow_duplicate(self, value: bool) -> JSONFormatter:
        """Allow duplicate object field names (only the last one is preserved)."""
        if not isinstance(value, bool):
            raise TypeError("allow_duplicate should be a bool")
        self.options["ALLOW_DUPLICATE"] = value
        return self

    def strip_outer_array(self, value: bool) -> JSONFormatter:
        """Remove the outer array brackets and load each element as its own row."""
        if not isinstance(value, bool):
            raise TypeError("strip_outer_array should be a bool")
        self.options["STRIP_OUTER_ARRAY"] = value
        return self

    def strip_null_values(self, value: bool) -> JSONFormatter:
        """Remove object fields or array elements containing SQL NULL."""
        if not isinstance(value, bool):
            raise TypeError("strip_null_values should be a bool")
        self.options["STRIP_NULL_VALUES"] = value
        return self

    def replace_invalid_characters(self, value: bool) -> JSONFormatter:
        """Replace invalid UTF-8 characters with the Unicode replacement character."""
        if not isinstance(value, bool):
            raise TypeError("replace_invalid_characters should be a bool")
        self.options["REPLACE_INVALID_CHARACTERS"] = value
        return self

    def ignore_utf8_errors(self, value: bool) -> JSONFormatter:
        """Replace UTF-8 encoding errors with the Unicode replacement character."""
        if not isinstance(value, bool):
            raise TypeError("ignore_utf8_errors should be a bool")
        self.options["IGNORE_UTF8_ERRORS"] = value
        return self

    def skip_byte_order_mark(self, value: bool) -> JSONFormatter:
        """Skip any byte order mark (BOM) present in an input file."""
        if not isinstance(value, bool):
            raise TypeError("skip_byte_order_mark should be a bool")
        self.options["SKIP_BYTE_ORDER_MARK"] = value
        return self


class PARQUETFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = "parquet"

    def snappy_compression(self, comp: bool) -> PARQUETFormatter:
        """Enable, or disable snappy compression"""
        if not isinstance(comp, bool):
            raise TypeError("Comp should be a Boolean value")
        self.options["SNAPPY_COMPRESSION"] = translate_bool(comp)
        return self

    def compression(self, comp: str) -> PARQUETFormatter:
        """
        Set compression type
        """
        if not isinstance(comp, str):
            raise TypeError("Comp should be a str value")
        self.options["COMPRESSION"] = comp
        return self

    def binary_as_text(self, value: bool) -> PARQUETFormatter:
        """Enable, or disable binary as text"""
        if not isinstance(value, bool):
            raise TypeError("binary_as_text should be a Boolean value")
        self.options["BINARY_AS_TEXT"] = translate_bool(value)
        return self


class ExternalStage(ClauseElement, FromClauseRole):
    """External Stage descriptor"""

    __visit_name__ = "external_stage"
    _hide_froms = ()

    @staticmethod
    def prepare_namespace(namespace: str) -> str:
        return f"{namespace}." if not namespace.endswith(".") else namespace

    @staticmethod
    def prepare_path(path: str) -> str:
        return f"/{path}" if not path.startswith("/") else path

    def __init__(
        self,
        name: str,
        path: str | None = None,
        namespace: str | None = None,
        file_format: str | None = None,
    ) -> None:
        self.name = name
        self.path = self.prepare_path(path) if path else ""
        self.namespace = self.prepare_namespace(namespace) if namespace else ""
        self.file_format = file_format

    def __repr__(self) -> str:
        return f"@{self.namespace}{self.name}{self.path} ({self.file_format})"

    @classmethod
    def from_parent_stage(
        cls,
        parent_stage: ExternalStage,
        path: str,
        file_format: str | None = None,
    ) -> ExternalStage:
        """
        Extend an existing parent stage (with or without path) with an
        additional sub-path
        """
        return cls(
            parent_stage.name,
            f"{parent_stage.path}/{path}",
            parent_stage.namespace,
            file_format,
        )


class CreateFileFormat(DDLElement):
    """
    Encapsulates a CREATE FILE FORMAT statement; using a format description (as in
    a COPY INTO statement) and a format name.
    """

    __visit_name__ = "create_file_format"

    def __init__(
        self,
        format_name: str,
        formatter: CopyFormatter,
        replace_if_exists: bool = False,
        if_not_exists: bool = False,
        comment: str | None = None,
    ) -> None:
        super().__init__()
        if replace_if_exists and if_not_exists:
            raise ValueError(
                "replace_if_exists and if_not_exists are mutually exclusive; "
                "Snowflake does not allow OR REPLACE together with IF NOT EXISTS."
            )
        self.format_name = format_name
        self.formatter = formatter
        self.replace_if_exists = replace_if_exists
        self.if_not_exists = if_not_exists
        self.comment = comment


class CreateStage(DDLElement):
    """
    Encapsulates a CREATE STAGE statement, using a container (physical base for the
    stage) and the actual ExternalStage object.
    """

    __visit_name__ = "create_stage"

    def __init__(
        self,
        container: CloudStorageLocation | ExternalStage,
        stage: ExternalStage,
        replace_if_exists: bool = False,
        *,
        temporary: bool = False,
    ) -> None:
        super().__init__()
        self.container = container
        self.temporary = temporary
        self.stage = stage
        self.replace_if_exists = replace_if_exists


class CloudStorageLocation(ClauseElement):
    """Base class for cloud storage URI locations used in COPY INTO statements."""

    @classmethod
    def from_uri(cls, uri: str) -> CloudStorageLocation:
        raise NotImplementedError


class AWSBucket(CloudStorageLocation):
    """AWS S3 bucket descriptor"""

    __visit_name__ = "aws_bucket"

    def __init__(self, bucket: str, path: str | None = None) -> None:
        self.bucket = bucket
        self.path = path
        self.encryption_used: dict[str, Any] = {}
        self.credentials_used: dict[str, Any] = {}

    @classmethod
    def from_uri(cls, uri: str) -> AWSBucket:
        if uri[0:5] != "s3://":
            raise ValueError(f"Invalid AWS bucket URI: {uri}")
        b = uri[5:].split("/", 1)
        if len(b) == 1:
            bucket, path = b[0], None
        else:
            bucket, path = b
        return cls(bucket, path)

    def __repr__(self) -> str:
        credentials = "CREDENTIALS=({})".format(
            " ".join(
                f"{n}='{_redact_option(n, v)}'"
                for n, v in self.credentials_used.items()
            )
        )
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                (f"{n}='{_redact_option(n, v)}'" if isinstance(v, str) else f"{n}={v}")
                for n, v in self.encryption_used.items()
            )
        )
        uri = "'s3://{}{}'".format(self.bucket, f"/{self.path}" if self.path else "")
        return "{}{}{}".format(
            uri,
            f" {credentials}" if self.credentials_used else "",
            f" {encryption}" if self.encryption_used else "",
        )

    def credentials(
        self,
        aws_role: str | None = None,
        aws_key_id: str | None = None,
        aws_secret_key: str | None = None,
        aws_token: str | None = None,
    ) -> AWSBucket:
        if aws_role is None and (aws_key_id is None and aws_secret_key is None):
            raise ValueError(
                "Either 'aws_role', or aws_key_id and aws_secret_key has to be supplied"
            )
        if aws_role:
            self.credentials_used = {"AWS_ROLE": aws_role}
        else:
            self.credentials_used = {
                "AWS_SECRET_KEY": aws_secret_key,
                "AWS_KEY_ID": aws_key_id,
            }
            if aws_token:
                self.credentials_used["AWS_TOKEN"] = aws_token
        return self

    def encryption_aws_cse(self, master_key: str) -> AWSBucket:
        self.encryption_used = {"TYPE": "AWS_CSE", "MASTER_KEY": master_key}
        return self

    def encryption_aws_sse_s3(self) -> AWSBucket:
        self.encryption_used = {"TYPE": "AWS_SSE_S3"}
        return self

    def encryption_aws_sse_kms(self, kms_key_id: str | None = None) -> AWSBucket:
        self.encryption_used = {"TYPE": "AWS_SSE_KMS"}
        if kms_key_id:
            self.encryption_used["KMS_KEY_ID"] = kms_key_id
        return self


class AzureContainer(CloudStorageLocation):
    """Microsoft Azure Container descriptor"""

    __visit_name__ = "azure_container"

    def __init__(self, account: str, container: str, path: str | None = None) -> None:
        self.account = account
        self.container = container
        self.path = path
        self.encryption_used: dict[str, Any] = {}
        self.credentials_used: dict[str, Any] = {}

    @classmethod
    def from_uri(cls, uri: str) -> AzureContainer:
        if uri[0:8] != "azure://":
            raise ValueError(f"Invalid Azure Container URI: {uri}")
        account, uri = uri[8:].split(".", 1)
        if uri[0:22] != "blob.core.windows.net/":
            raise ValueError(f"Invalid Azure Container URI: {uri}")
        b = uri[22:].split("/", 1)
        if len(b) == 1:
            container, path = b[0], None
        else:
            container, path = b
        return cls(account, container, path)

    def __repr__(self) -> str:
        credentials = "CREDENTIALS=({})".format(
            " ".join(
                f"{n}='{_redact_option(n, v)}'"
                for n, v in self.credentials_used.items()
            )
        )
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                (f"{n}='{_redact_option(n, v)}'" if isinstance(v, str) else f"{n}={v}")
                for n, v in self.encryption_used.items()
            )
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            self.account, self.container, f"/{self.path}" if self.path else ""
        )
        return "{}{}{}".format(
            uri,
            f" {credentials}" if self.credentials_used else "",
            f" {encryption}" if self.encryption_used else "",
        )

    def credentials(self, azure_sas_token: str) -> AzureContainer:
        self.credentials_used = {"AZURE_SAS_TOKEN": azure_sas_token}
        return self

    def encryption_azure_cse(self, master_key: str) -> AzureContainer:
        self.encryption_used = {"TYPE": "AZURE_CSE", "MASTER_KEY": master_key}
        return self


class GCSBucket(CloudStorageLocation):
    """Google Cloud Storage bucket descriptor"""

    __visit_name__ = "gcs_bucket"

    def __init__(self, bucket: str, path: str | None = None) -> None:
        self.bucket = bucket
        self.path = path
        self.encryption_used: dict[str, Any] = {}

    @classmethod
    def from_uri(cls, uri: str) -> GCSBucket:
        if uri[0:6] != "gcs://":
            raise ValueError(f"Invalid GCS bucket URI: {uri}")
        b = uri[6:].split("/", 1)
        if len(b) == 1:
            bucket, path = b[0], None
        else:
            bucket, path = b
        return cls(bucket, path)

    def __repr__(self) -> str:
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                (f"{n}='{_redact_option(n, v)}'" if isinstance(v, str) else f"{n}={v}")
                for n, v in self.encryption_used.items()
            )
        )
        uri = "'gcs://{}{}'".format(self.bucket, f"/{self.path}" if self.path else "")
        return "{}{}".format(uri, f" {encryption}" if self.encryption_used else "")

    def encryption_gcs_sse_kms(self, kms_key_id: str | None = None) -> GCSBucket:
        self.encryption_used = {"TYPE": "GCS_SSE_KMS"}
        if kms_key_id:
            self.encryption_used["KMS_KEY_ID"] = kms_key_id
        return self

    def encryption_none(self) -> GCSBucket:
        self.encryption_used = {"TYPE": "NONE"}
        return self


CopyIntoStorage = CopyInto
