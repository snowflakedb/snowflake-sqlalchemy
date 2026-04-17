#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""
Alembic utilities for Snowflake SQLAlchemy.

Usage in your Alembic ``env.py``::

    from snowflake.sqlalchemy.alembic_util import render_item as snowflake_render_item

    context.configure(
        ...,
        render_item=snowflake_render_item,
    )

Without this hook, Alembic serialises ``quoted_name("mycol", True)`` as the
plain string ``"mycol"``, which loses the case-sensitivity signal.  The
generated migration would create a case-insensitive ``MYCOL`` column in
Snowflake instead of the intended case-sensitive ``"mycol"``.

The ``render_item`` function returns ``False`` for all items it does not handle,
so Alembic falls back to its default renderer.  It is safe to use as a
drop-in ``render_item`` hook even when only some columns are case-sensitive.

Hard limit: Alembic has no dialect-level rendering hook.  The ``render_item``
callback in ``env.py`` is the only injection point and requires a two-line
opt-in per project.
"""

from sqlalchemy.sql.elements import quoted_name


def render_item(type_, obj, autogen_context):
    """
    Alembic ``render_item`` hook that preserves case-sensitive (``quoted_name``)
    column names in generated migration files.

    Parameters
    ----------
    type_:
        The item type string provided by Alembic (e.g. ``"column"``,
        ``"table"``, ``"type"``).
    obj:
        The SQLAlchemy object being rendered.
    autogen_context:
        The Alembic autogeneration context.

    Returns
    -------
    str or False
        A rendered Python expression string when the column has a
        ``quoted_name`` name with ``quote=True``; ``False`` otherwise so
        Alembic uses its default renderer.
    """
    if type_ == "column" and isinstance(obj.name, quoted_name) and obj.name.quote:
        col_name = str(obj.name)
        # Render the column type using Alembic's own type renderer.
        # Note: autogenerate_module.render_column_type is an internal Alembic API
        # that may change across releases. The fallback to repr() provides safety.
        try:
            rendered_type = autogen_context.opts[
                "autogenerate_module"
            ].render_column_type(obj.type, autogen_context)
        except (KeyError, AttributeError):
            # Fall back to plain repr if autogenerate_module not available
            rendered_type = repr(obj.type)

        # Build the column definition starting with name and type
        parts = [f"sa.sql.elements.quoted_name({col_name!r}, True)", rendered_type]

        # Add important column attributes that differ from defaults
        # nullable defaults to True, so only emit nullable=False
        if not obj.nullable:
            parts.append("nullable=False")

        # Emit primary_key when True
        if obj.primary_key:
            parts.append("primary_key=True")

        # Emit server_default when set
        if obj.server_default is not None:
            # Render the server_default using Alembic's renderer if available
            try:
                rendered_default = autogen_context.opts[
                    "autogenerate_module"
                ].render_server_default(obj.server_default, autogen_context)
                parts.append(f"server_default={rendered_default}")
            except (KeyError, AttributeError):
                # Fall back to repr if renderer not available
                parts.append(f"server_default={repr(obj.server_default)}")

        return f"sa.Column({', '.join(parts)})"
    return False
