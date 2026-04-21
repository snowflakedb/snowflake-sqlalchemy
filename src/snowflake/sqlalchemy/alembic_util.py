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


class _ReprExpr(str):
    """str subclass whose ``repr()`` emits an arbitrary Python expression.

    Alembic's ``_render_column`` renders the column name via the format slot
    ``%(name)r``, which calls ``repr(_ident(column.name))``.  ``_ident``
    returns the name as a plain ``str``, so ``repr()`` would produce
    ``'mycol'`` — losing the ``quote=True`` signal.

    By replacing ``column.name`` with a ``_ReprExpr`` whose ``__repr__``
    returns the ``quoted_name(...)`` expression, we get the correct output
    while letting Alembic handle every other column attribute (type, nullable,
    autoincrement, comment, server_default, column kwargs, …).
    """

    def __new__(cls, value: str, expr: str):
        obj = super().__new__(cls, value)
        obj._expr = expr
        return obj

    def __repr__(self) -> str:
        return self._expr


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
        quoted_expr = f"sa.sql.elements.quoted_name({col_name!r}, True)"

        # Primary path: delegate all column attribute rendering to Alembic's
        # own _render_column, only injecting our quoted_name(...) expression
        # in place of the plain column name.
        #
        # _render_column calls _user_defined_render first, which would invoke
        # our hook again — infinite recursion.  Setting opts["render_item"]
        # to None makes _user_defined_render skip it (falsy check).
        original_name = obj.name
        had_render_item = "render_item" in autogen_context.opts
        original_render_item = autogen_context.opts.get("render_item")
        obj.name = _ReprExpr(col_name, quoted_expr)
        autogen_context.opts["render_item"] = None
        try:
            from alembic.autogenerate.render import _render_column

            return _render_column(obj, autogen_context)
        except Exception:
            pass
        finally:
            obj.name = original_name
            if had_render_item:
                autogen_context.opts["render_item"] = original_render_item
            else:
                autogen_context.opts.pop("render_item", None)

        # Fallback: Alembic's internal _render_column is unavailable or raised.
        # Manually render the most common column attributes.
        parts = [quoted_expr]
        try:
            rendered_type = autogen_context.opts[
                "autogenerate_module"
            ].render_column_type(obj.type, autogen_context)
        except (KeyError, AttributeError):
            rendered_type = repr(obj.type)
        parts.append(rendered_type)

        if not obj.nullable:
            parts.append("nullable=False")
        if obj.primary_key:
            parts.append("primary_key=True")
        if obj.server_default is not None:
            try:
                rendered_default = autogen_context.opts[
                    "autogenerate_module"
                ].render_server_default(obj.server_default, autogen_context)
                parts.append(f"server_default={rendered_default}")
            except (KeyError, AttributeError):
                parts.append(f"server_default={repr(obj.server_default)}")

        return f"sa.Column({', '.join(parts)})"
    return False
