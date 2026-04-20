#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""ORM utilities for efficient bulk inserts with Snowflake.

This module provides two components that together solve the ``bulk_save_objects``
batch-fragmentation problem (SNOW-893080, GitHub #441):

**SnowflakeBase / snowflake_declarative_base**
    A custom declarative base whose ``__init__`` pre-populates every mapped
    column that has no server-side or callable default with its Python-level
    scalar default (or ``None``).  This ensures that every model instance
    always has the same set of column keys in its ``__dict__`` (the ORM
    ``state_dict``), regardless of which kwargs the caller supplied.

**SnowflakeSession**
    A ``Session`` subclass that overrides ``bulk_save_objects`` to pass
    ``render_nulls=True`` to ``_bulk_save_mappings``.  Without this flag,
    SQLAlchemy strips ``None`` values from the parameter dict before grouping
    rows into INSERT batches, so objects with ``col=None`` and objects with
    ``col='hello'`` still produce different parameter-key sets and are emitted
    as separate INSERT statements.

Together, both parts are required: the base class normalises the key set, and
the session override prevents the normalised ``None`` values from being stripped
before grouping.

Limitation
----------
Columns with ``server_default``, callable Python defaults (``default=fn``), or
SQL-expression defaults (``default=func.now()``) are intentionally left absent
from the pre-populated ``state_dict``.  If some objects supply an explicit value
for such a column while others do not, those objects will still produce different
parameter-key sets and may be placed in separate INSERT batches.  This is the
same behaviour as stock SQLAlchemy and is not made worse by this module.
"""

from __future__ import annotations

import itertools

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, attributes

from .compat import IS_VERSION_20


def _snowflake_constructor(self, **kwargs):
    """Custom ORM instance constructor that pre-populates mapped columns.

    Mirrors SA's own ``mapper._insert_cols_as_none`` logic (SA 2.x
    mapper.py L2764-2776; SA 1.4 mapper.py L2233-2249): primary keys,
    server defaults, all client-side defaults (callable and
    SQL-expression), and ``should_evaluate_none`` columns are intentionally
    left absent from ``state_dict`` so that their normal SA handling is
    preserved.

    For all remaining mapped columns:
    - columns with a scalar Python ``default`` are pre-populated with
      that scalar value;
    - columns with no default at all are pre-populated with ``None``.

    This is intentionally called *after* SA's instrumented init so that
    SA's event hooks run first.  User-supplied kwargs always take
    precedence — we skip any column whose attribute key was already
    present in ``kwargs``.
    """
    cls_ = type(self)

    # Apply user-supplied kwargs first (mirrors _declarative_constructor).
    for k, v in kwargs.items():
        if not hasattr(cls_, k):
            raise TypeError(f"{k!r} is an invalid keyword argument for {cls_.__name__}")
        setattr(self, k, v)

    # Pre-populate remaining column attributes.
    # Follows the same exclusion logic as SA's mapper._insert_cols_as_none.
    mapper = sa_inspect(cls_).mapper
    for attr in mapper.column_attrs:
        if attr.key in kwargs:
            # User supplied a value — do not overwrite.
            continue

        col = attr.columns[0]

        if col.primary_key:
            # Leave absent: PKs are either user-supplied or DB-generated.
            # Setting None would corrupt autoincrement PK handling and send
            # an explicit NULL PK in the INSERT.
            continue

        if col.server_default is not None:
            # Leave absent: server default must fire on the DB side.
            # If we include an explicit NULL in the INSERT, SA's crud.py
            # _scan_cols fires the "column IS in parameters" branch and
            # sends NULL, overriding the server default entirely.
            continue

        if col.type.should_evaluate_none:
            # Leave absent: JSON and similar types use should_evaluate_none=True
            # to distinguish "store JSON null" from "omit the column".
            continue

        if col.default is not None:
            if col.default.is_scalar:
                # Pre-populate with the known Python literal so that all
                # objects share this key in their state_dict.
                setattr(self, attr.key, col.default.arg)
            # else: callable or SQL-expression default — leave absent so SA
            # invokes it with an ExecutionContext (or sequences) at INSERT time.
        else:
            # No default of any kind: pre-populate with None.
            # render_nulls=True (in SnowflakeSession) then includes this
            # column in every INSERT, unifying the parameter-key set.
            setattr(self, attr.key, None)


def snowflake_declarative_base(**kw):
    """Create a declarative base with the Snowflake bulk-insert constructor.

    Works with both SQLAlchemy 1.4 and 2.x.  The returned base class
    installs ``_snowflake_constructor`` as ``__init__`` on every mapped
    model, so that every instance pre-populates all plain-nullable columns
    with ``None`` (or their scalar default) at construction time.

    Use this together with :class:`SnowflakeSession` to enable single-batch
    ``bulk_save_objects`` inserts for models with nullable optional columns.

    Parameters
    ----------
    **kw:
        Forwarded verbatim to ``sqlalchemy.orm.declarative_base()``.
    """
    from sqlalchemy.orm import declarative_base

    return declarative_base(constructor=_snowflake_constructor, **kw)


# SA 2.x only: class-based DeclarativeBase subclass.
# SA 1.4 does not have DeclarativeBase so the class definition is guarded.
if IS_VERSION_20:
    from sqlalchemy.orm import DeclarativeBase

    class SnowflakeBase(DeclarativeBase):
        """Declarative base for Snowflake ORM models with efficient bulk inserts.

        Subclass your models from ``SnowflakeBase`` (SQLAlchemy 2.x) instead
        of the default ``DeclarativeBase`` to enable single-batch
        ``bulk_save_objects`` behaviour.

        Use together with :class:`SnowflakeSession`.

        Example::

            from snowflake.sqlalchemy import SnowflakeBase, SnowflakeSession

            class MyModel(SnowflakeBase):
                __tablename__ = "my_model"
                id = Column(Integer, primary_key=True)
                name = Column(String)   # nullable, no default

            session = SnowflakeSession(bind=engine)
            session.bulk_save_objects([MyModel(id=1), MyModel(id=2, name="foo")])
            # Both objects go in a single INSERT (executemany).
        """

        def __init__(self, **kwargs):
            _snowflake_constructor(self, **kwargs)


class SnowflakeSession(Session):
    """Session subclass enabling efficient bulk inserts.

    Overrides :meth:`bulk_save_objects` to pass ``render_nulls=True`` to
    the internal ``_bulk_save_mappings`` call.  This prevents ``None``
    values that were pre-populated by ``_snowflake_constructor`` from being
    stripped out of the INSERT parameter dict, so that all objects produce
    the same parameter-key set and are placed in a single ``executemany``
    INSERT batch.

    Must be used together with :class:`SnowflakeBase` (SA 2.x) or
    :func:`snowflake_declarative_base` (SA 1.4 / 2.x) for full effect.

    SA version compatibility
    ~~~~~~~~~~~~~~~~~~~~~~~~
    ``Session._bulk_save_mappings`` is called with keyword arguments, which
    is valid for both SA 1.4 (positional-or-keyword) and SA 2.x
    (keyword-only after ``*``).  Verified against SA 1.4.54 and SA 2.0.48.

    Note: ``super().bulk_save_objects()`` hardcodes ``render_nulls=False``
    with no override hook (SA 2.x session.py:4571; SA 1.4 equivalent).
    This override replicates the ``itertools.groupby`` dispatch logic from
    both SA versions to inject ``render_nulls=True``.
    """

    def bulk_save_objects(
        self,
        objects,
        return_defaults=False,
        update_changed_only=True,
        preserve_order=True,
    ):
        """Bulk-save ORM objects using a single batched INSERT per mapper.

        Identical to :meth:`sqlalchemy.orm.Session.bulk_save_objects` except
        that ``render_nulls=True`` is passed to the underlying
        ``_bulk_save_mappings`` call.  See the class docstring for details.
        """
        obj_states = (attributes.instance_state(obj) for obj in objects)

        if not preserve_order:
            # Group common mappers/persistence states together so that
            # itertools.groupby yields one group per mapper type.
            obj_states = sorted(
                obj_states,
                key=lambda state: (id(state.mapper), state.key is not None),
            )

        def grouping_key(state):
            return (state.mapper, state.key is not None)

        for (mapper, isupdate), states in itertools.groupby(obj_states, grouping_key):
            self._bulk_save_mappings(
                mapper,
                states,
                isupdate=isupdate,
                isstates=True,
                return_defaults=return_defaults,
                update_changed_only=update_changed_only,
                render_nulls=True,  # key difference from stock Session
            )
