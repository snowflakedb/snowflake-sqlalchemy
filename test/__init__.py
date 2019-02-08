from snowflake.sqlalchemy import SnowflakeDialect


def sql_compile(sql_command):
    """Returns the compiled SQL command with literals bound and without new lines"""
    return str(sql_command.compile(dialect=SnowflakeDialect(),
                                   compile_kwargs={'literal_binds': True})).replace('\n', '')
