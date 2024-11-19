#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from typing import List

from sqlalchemy.exc import ArgumentError


class NoPrimaryKeyError(ArgumentError):
    def __init__(self, target: str):
        super().__init__(f"Table {target} required primary key.")


class UnsupportedPrimaryKeysAndForeignKeysError(ArgumentError):
    def __init__(self, target: str):
        super().__init__(f"Primary key and foreign keys are not supported in {target}.")


class RequiredParametersNotProvidedError(ArgumentError):
    def __init__(self, target: str, parameters: List[str]):
        super().__init__(
            f"{target} requires the following parameters: %s." % ", ".join(parameters)
        )


class UnexpectedTableOptionKeyError(ArgumentError):
    def __init__(self, expected: str, actual: str):
        super().__init__(f"Expected table option {expected} but got {actual}.")


class OptionKeyNotProvidedError(ArgumentError):
    def __init__(self, target: str):
        super().__init__(
            f"Expected option key in {target} option but got NoneType instead."
        )


class UnexpectedOptionParameterTypeError(ArgumentError):
    def __init__(self, parameter_name: str, target: str, types: List[str]):
        super().__init__(
            f"Parameter {parameter_name} of {target} requires to be one"
            f" of following types: {', '.join(types)}."
        )


class CustomOptionsAreOnlySupportedOnSnowflakeTables(ArgumentError):
    def __init__(self):
        super().__init__(
            "Identifier, Literal, TargetLag and other custom options are only supported on Snowflake tables."
        )


class UnexpectedOptionTypeError(ArgumentError):
    def __init__(self, options: List[str]):
        super().__init__(
            f"The following options are either unsupported or should be defined using a Snowflake table: {', '.join(options)}."
        )


class InvalidTableParameterTypeError(ArgumentError):
    def __init__(self, name: str, input_type: str, expected_types: List[str]):
        expected_types_str = "', '".join(expected_types)
        super().__init__(
            f"Invalid parameter type '{input_type}' provided for '{name}'. "
            f"Expected one of the following types: '{expected_types_str}'.\n"
        )


class MultipleErrors(ArgumentError):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return "".join(str(e) for e in self.errors)


class StructuredTypeNotSupportedInTableColumnsError(ArgumentError):
    def __init__(self, table_type: str, table_name: str, column_name: str):
        super().__init__(
            f"Column '{column_name}' is of a structured type, which is only supported on Iceberg tables. "
            f"The table '{table_name}' is of type '{table_type}', not Iceberg."
        )
