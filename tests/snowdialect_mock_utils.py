#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from unittest.mock import Mock


class MockError(Exception):
    pass


class MockDisconnect(MockError):
    pass


class MockExitIsh(BaseException):
    pass


def mock_connection(mock_query_map):
    def mock_cursor():
        def execute(sql_str, *args, **kwargs):
            if conn.explode == "execute":
                raise MockDisconnect("Lost the DB connection on execute")
            for key, (rows, descriptions) in mock_query_map.items():
                if key.lower() in str(sql_str).lower():
                    # Directly assign the correct results to fetch methods
                    cursor.description = descriptions
                    cursor.fetchall.return_value = rows
                    cursor.fetchone.return_value = rows[0] if rows else None
                    return cursor
            cursor.description = None
            cursor.fetchall.return_value = []
            cursor.fetchone.return_value = None

            return cursor

        def close():
            cursor.fetchall = Mock(side_effect=MockError("cursor closed"))
            cursor.fetchone = Mock(side_effect=MockError("cursor closed"))

        cursor = Mock()
        cursor.execute = Mock(side_effect=execute)
        cursor.close = Mock(side_effect=close)
        # Set initial fetchall/fetchone to empty returns
        cursor.fetchall = Mock(return_value=[])
        cursor.fetchone = Mock(return_value=None)
        return cursor

    def cursor():
        return mock_cursor()

    # ... rollback, commit as before ...
    conn = Mock()
    conn.rollback = Mock()
    conn.commit = Mock()
    conn.cursor = Mock(side_effect=cursor)
    conn.explode = None
    return conn


def MockDBAPI(mock_query_map):
    connections = []
    stopped = [False]

    def connect(*args, **kwargs):
        if stopped[0]:
            raise MockDisconnect("database is stopped")
        conn = mock_connection(mock_query_map)
        connections.append(conn)
        return conn

    def shutdown(explode="execute", stop=False):
        stopped[0] = stop
        for c in connections:
            c.explode = explode

    def restart():
        stopped[0] = False
        connections[:] = []

    def dispose():
        stopped[0] = False
        for c in connections:
            c.explode = None
        connections[:] = []

    return Mock(
        connect=Mock(side_effect=connect),
        shutdown=Mock(side_effect=shutdown),
        dispose=Mock(side_effect=dispose),
        restart=Mock(side_effect=restart),
        paramstyle="named",
        connections=connections,
        Error=MockError,
    )


def get_select_information_schema_columns():
    return [
        ("CURRENT_DATABASE()", None, None, None, None, None),
        ("CURRENT_SCHEMA()", None, None, None, None, None),
    ]


def get_desc_table_columns():
    return [
        ("TABLE_NAME", None, None, None, None, None),
        ("COLUMN_NAME", None, None, None, None, None),
        ("DATA_TYPE", None, None, None, None, None),
        ("CHARACTER_MAXIMUM_LENGTH", None, None, None, None, None),
        ("NUMERIC_PRECISION", None, None, None, None, None),
        ("NUMERIC_SCALE", None, None, None, None, None),
        ("IS_NULLABLE", None, None, None, None, None),
        ("COLUMN_DEFAULT", None, None, None, None, None),
        ("IS_IDENTITY", None, None, None, None, None),
        ("COMMENT", None, None, None, None, None),
        ("IDENTITY_START", None, None, None, None, None),
        ("IDENTITY_INCREMENT", None, None, None, None, None),
    ]


def get_show_table_in_schema_columns():
    return [
        ("created_on", None, None, None, None, None),
        ("name", None, None, None, None, None),
        ("database_name", None, None, None, None, None),
        ("schema_name", None, None, None, None, None),
        ("kind", None, None, None, None, None),
        ("comment", None, None, None, None, None),
        ("cluster_by", None, None, None, None, None),
        ("rows", None, None, None, None, None),
        ("bytes", None, None, None, None, None),
        ("owner", None, None, None, None, None),
        ("retention_time", None, None, None, None, None),
        ("automatic_clustering", None, None, None, None, None),
        ("change_tracking", None, None, None, None, None),
        ("is_external", None, None, None, None, None),
        ("enable_schema_evolution", None, None, None, None, None),
        ("owner_role_type", None, None, None, None, None),
        ("is_event", None, None, None, None, None),
        ("is_hybrid", None, None, None, None, None),
        ("is_iceberg", None, None, None, None, None),
        ("is_dynamic", None, None, None, None, None),
        ("is_immutable", None, None, None, None, None),
    ]


def get_show_primary_key_columns():
    return [
        ("created_on", None, None, None, None, None),
        ("database_name", None, None, None, None, None),
        ("schema_name", None, None, None, None, None),
        ("table_name", None, None, None, None, None),
        ("column_name", None, None, None, None, None),
        ("key_sequence", None, None, None, None, None),
        ("constraint_name", None, None, None, None, None),
    ]


def make_mock_query_map(
    current_schema, schema_columns_rows, schema_tables_info, primary_keys_rows
):
    return {
        "select current_database(), current_schema()": (
            current_schema,
            get_select_information_schema_columns(),
        ),
        "select /* sqlalchemy:_get_schema_columns */": (
            schema_columns_rows,
            get_desc_table_columns(),
        ),
        "show /* sqlalchemy:get_schema_tables_info */": (
            schema_tables_info,
            get_show_table_in_schema_columns(),
        ),
        "show /* sqlalchemy:_get_schema_primary_keys */": (
            primary_keys_rows,
            get_show_primary_key_columns(),
        ),
    }
