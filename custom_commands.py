#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from collections.abc import Sequence
from sqlalchemy import true, false
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.util.compat import string_types
from sqlalchemy.sql.elements import ClauseElement


NoneType = type(None)


def translate_bool(bln):
    if bln:
        return true()
    return false()


class MergeInto(UpdateBase):
    __visit_name__ = 'merge_into'
    _bind = None

    def __init__(self, target, source, on):
        self.target = target
        self.source = source
        self.on = on
        self.clauses = []

    class clause(ClauseElement):
        __visit_name__ = 'merge_into_clause'

        def __init__(self, command):
            self.set = {}
            self.predicate = None
            self.command = command

        def __repr__(self):
            if self.command == 'INSERT':
                sets, sets_tos = zip(*self.set.items())
                return "WHEN NOT MATCHED%s THEN %s (%s) VALUES (%s)" % (
                    " AND %s" % self.predicate if self.predicate is not None else "",
                    self.command,
                    ", ".join(sets),
                    ", ".join(map(str, sets_tos)))
            else:
                # WHEN MATCHED clause
                sets = ", ".join(["%s = %s" % (set[0], set[1]) for set in self.set.items()]) if self.set else ""
                return "WHEN MATCHED%s THEN %s%s" % (" AND %s" % self.predicate if self.predicate is not None else "",
                                                     self.command,
                                                     " SET %s" % sets if self.set else "")

        def values(self, **kwargs):
            self.set = kwargs
            return self

        def where(self, expr):
            self.predicate = expr
            return self

    def __repr__(self):
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return "MERGE INTO %s USING %s ON %s" % (self.target, self.source, self.on) + (
            ' ' + clauses if clauses else ''
        )

    def when_matched_then_update(self):
        clause = self.clause('UPDATE')
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self):
        clause = self.clause('DELETE')
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self):
        clause = self.clause('INSERT')
        self.clauses.append(clause)
        return clause


class CopyInto(UpdateBase):
    """Copy Into Command base class, for documentation see:
    https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html"""

    __visit_name__ = 'copy_into'
    _bind = None

    def __init__(self, from_, into, formatter):
        self.from_ = from_
        self.into = into
        self.formatter = formatter
        self.copy_options = {}

    def __repr__(self):
        options = (' ' + ' '.join(["{} = {}".format(n, str(v)) for n, v in
                                   self.copy_options.items()])) if self.copy_options else ''
        return "COPY INTO {} FROM {} {}{}".format(self.into.__repr__(),
                                                  self.from_.__repr__(),
                                                  self.formatter.__repr__(),
                                                  options)

    def bind(self):
        return None

    def overwrite(self, overwrite):
        if not isinstance(overwrite, bool):
            raise TypeError("Parameter overwrite should  be a boolean value")
        self.copy_options.update({'OVERWRITE': translate_bool(overwrite)})

    def single(self, single_file):
        if not isinstance(single_file, bool):
            raise TypeError("Parameter single_file should  be a boolean value")
        self.copy_options.update({'SINGLE': translate_bool(single_file)})

    def maxfilesize(self, max_size):
        if not isinstance(max_size, int):
            raise TypeError("Parameter max_size should be an integer value")
        self.copy_options.update({'MAX_FILE_SIZE': max_size})


class CopyFormatter(ClauseElement):
    __visit_name__ = 'copy_formatter'

    def __init__(self):
        self.options = {}

    def __repr__(self):
        return 'FILE_FORMAT=(TYPE={}{})'.format(
            self.file_format,
            (' ' + ' '.join([("{} = '{}'" if isinstance(value, str) else "{} = {}").format(name, str(value))
                             for name, value in self.options.items()])) if self.options else ""
        )


class CSVFormatter(CopyFormatter):
    file_format = 'csv'

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = ['auto', 'gzip', 'bz2', 'brotli', 'zstd', 'deflate', 'raw_deflate', None]
        if comp_type not in _available_options:
            raise TypeError("Compression type should be one of : {}".format(_available_options))
        self.options['COMPRESSION'] = comp_type
        return self

    def record_delimiter(self, deli_type):
        """Character that separates records in an unloaded file."""
        if not isinstance(deli_type, (int, string_types)) \
                or (isinstance(deli_type, string_types) and len(deli_type) != 1):
            raise TypeError("Record delimeter should be a single character, that is either a string, or a number")
        if isinstance(deli_type, int):
            self.options['RECORD_DELIMITER'] = hex(deli_type)
        else:
            self.options['RECORD_DELIMITER'] = deli_type
        return self

    def field_delimiter(self, deli_type):
        """Character that separates fields in an unloaded file."""
        if not isinstance(deli_type, (int, NoneType, string_types)) \
                or (isinstance(deli_type, string_types) and len(deli_type) != 1):
            raise TypeError("Field delimeter should be a single character, that is either a string, or a number")
        if isinstance(deli_type, int):
            self.options['FIELD_DELIMITER'] = hex(deli_type)
        else:
            self.options['FIELD_DELIMITER'] = deli_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service. """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options['FILE_EXTENSION'] = ext
        return self

    def date_format(self, dt_frmt):
        """String that defines the format of date values in the unloaded data files."""
        if not isinstance(dt_frmt, string_types):
            raise TypeError("Date format should be a string")
        self.options['DATE_FORMAT'] = dt_frmt
        return self

    def time_format(self, tm_frmt):
        """String that defines the format of time values in the unloaded data files."""
        if not isinstance(tm_frmt, string_types):
            raise TypeError("Time format should be a string")
        self.options['TIME_FORMAT'] = tm_frmt
        return self

    def timestamp_format(self, tmstmp_frmt):
        """String that defines the format of timestamp values in the unloaded data files."""
        if not isinstance(tmstmp_frmt, string_types):
            raise TypeError("Timestamp format should be a string")
        self.options['TIMESTAMP_FORMAT'] = tmstmp_frmt
        return self

    def binary_format(self, bin_fmt):
        """Character used as the escape character for any field values. The option can be used when unloading data
        from binary columns in a table. """
        if isinstance(bin_fmt, string_types):
            bin_fmt = bin_fmt.lower()
        _available_options = ['hex', 'base64', 'utf8']
        if bin_fmt not in _available_options:
            raise TypeError("Binary format should be one of : {}".format(_available_options))
        self.options['BINARY_FORMAT'] = bin_fmt
        return self

    def escape(self, esc):
        """Character used as the escape character for any field values."""
        if not isinstance(esc, (int, NoneType, string_types)) \
                or (isinstance(esc, string_types) and len(esc) != 1):
            raise TypeError("Escape should be a single character, that is either a string, or a number")
        if isinstance(esc, int):
            self.options['ESCAPE'] = hex(esc)
        else:
            self.options['ESCAPE'] = esc
        return self

    def escape_unenclosed_field(self, esc):
        """Single character string used as the escape character for unenclosed field values only."""
        if not isinstance(esc, (int, NoneType, string_types)) \
                or (isinstance(esc, string_types) and len(esc) != 1):
            raise TypeError(
                "Escape unenclosed field should be a single character, that is either a string, or a number")
        if isinstance(esc, int):
            self.options['ESCAPE_UNENCLOSED_FIELD'] = hex(esc)
        else:
            self.options['ESCAPE_UNENCLOSED_FIELD'] = esc
        return self

    def field_optionally_enclosed_by(self, enc):
        """Character used to enclose strings. Either None, ', or \"."""
        _available_options = [None, '\'', '"']
        if enc not in _available_options:
            raise TypeError("Enclosing string should be one of : {}".format(_available_options))
        self.options['FIELD_OPTIONALLY_ENCLOSED_BY'] = enc
        return self

    def null_if(self, null):
        """Copying into a table these strings will be replaced by a NULL, while copying out of Snowflake will replace
        NULL values with the first string"""
        if not isinstance(null, Sequence):
            raise TypeError('Parameter null should be an iterable')
        self.options['NULL_IF'] = tuple(null)
        return self


class JSONFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = 'json'

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = ['auto', 'gzip', 'bz2', 'brotli', 'zstd', 'deflate', 'raw_deflate', None]
        if comp_type not in _available_options:
            raise TypeError("Compression type should be one of : {}".format(_available_options))
        self.options['COMPRESSION'] = comp_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service. """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options['FILE_EXTENSION'] = ext
        return self


class PARQUETFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = 'parquet'

    def snappy_compression(self, comp):
        """Enable, or disable snappy compression"""
        if not isinstance(comp, bool):
            raise TypeError("Comp should be a Boolean value")
        self.options['SNAPPY_COMPRESSION'] = translate_bool(comp)
        return self


class ExternalStage(ClauseElement):
    """External Stage descriptor"""
    __visit_name__ = "external_stage"

    @staticmethod
    def prepare_namespace(namespace):
        return "{}.".format(namespace) if not namespace.endswith(".") else namespace

    @staticmethod
    def prepare_path(path):
        return "/{}".format(path) if not path.startswith("/") else path

    def __init__(self, name, path=None, namespace=None):
        self.name = name
        self.path = self.prepare_path(path) if path else ""
        self.namespace = self.prepare_namespace(namespace) if namespace else ""

    def __repr__(self):
        return "@{}{}{}".format(self.namespace, self.name, self.path)


class AWSBucket(ClauseElement):
    """AWS S3 bucket descriptor"""
    __visit_name__ = 'aws_bucket'

    def __init__(self, bucket, path=None):
        self.bucket = bucket
        self.path = path
        self.encryption_used = {}
        self.credentials_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:5] != 's3://':
            raise ValueError("Invalid AWS bucket URI: {}".format(uri))
        b = uri[5:].split('/', 1)
        if len(b) == 1:
            bucket, path = b[0], None
        else:
            bucket, path = b
        return cls(bucket, path)

    def __repr__(self):
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in self.credentials_used.items())
        )
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v)
                     for n, v in self.encryption_used.items())
        )
        uri = "'s3://{}{}'".format(self.bucket, '/' + self.path if self.path else "")
        return '{}{}{}'.format(uri,
                               ' ' + credentials if self.credentials_used else '',
                               ' ' + encryption if self.encryption_used else '')

    def credentials(self, aws_role=None, aws_key_id=None, aws_secret_key=None, aws_token=None):
        if aws_role is None and (aws_key_id is None and aws_secret_key is None):
            raise ValueError("Either 'aws_role', or aws_key_id and aws_secret_key has to be supplied")
        if aws_role:
            self.credentials_used = {'AWS_ROLE': aws_role}
        else:
            self.credentials_used = {'AWS_SECRET_KEY': aws_secret_key,
                                     'AWS_KEY_ID': aws_key_id}
            if aws_token:
                self.credentials_used['AWS_TOKEN'] = aws_token
        return self

    def encryption_aws_cse(self, master_key):
        self.encryption_used = {'TYPE': 'AWS_CSE',
                                'MASTER_KEY': master_key}
        return self

    def encryption_aws_sse_s3(self):
        self.encryption_used = {'TYPE': 'AWS_SSE_S3'}
        return self

    def encryption_aws_sse_kms(self, kms_key_id=None):
        self.encryption_used = {'TYPE': 'AWS_SSE_KMS'}
        if kms_key_id:
            self.encryption_used['KMS_KEY_ID'] = kms_key_id
        return self


class AzureContainer(ClauseElement):
    """Microsoft Azure Container descriptor"""
    __visit_name__ = 'azure_container'

    def __init__(self, account, container, path=None):
        self.account = account
        self.container = container
        self.path = path
        self.encryption_used = {}
        self.credential_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:8] != 'azure://':
            raise ValueError("Invalid Azure Container URI: {}".format(uri))
        account, uri = uri[8:].split('.', 1)
        if uri[0:22] != 'blob.core.windows.net/':
            raise ValueError("Invalid Azure Container URI: {}".format(uri))
        b = uri[22:].split('/', 1)
        if len(b) == 1:
            container, path = b[0], None
        else:
            container, path = b
        return cls(account, container, path)

    def __repr__(self):
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in self.credentials_used.items())
        )
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v) for n, v in
                     self.encryption_used.items())
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            self.account,
            self.container,
            '/' + self.path if self.path else ""
        )
        return uri + credentials if self.credentials_used else '' + encryption if self.encryption_used else ''

    def credentials(self, azure_sas_token):
        self.credentials_used = {'AZURE_SAS_TOKEN': azure_sas_token}
        return self

    def encryption_azure_cse(self, master_key):
        self.encryption_used = {'TYPE': 'AZURE_CSE', 'MASTER_KEY': master_key}
        return self


CopyIntoStorage = CopyInto
