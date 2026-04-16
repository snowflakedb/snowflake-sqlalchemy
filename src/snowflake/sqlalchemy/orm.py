#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import itertools
from typing import Any, Iterable

from sqlalchemy.orm import Session
from sqlalchemy.orm.base import _class_to_mapper
from sqlalchemy.orm.util import attributes

from snowflake.sqlalchemy.compat import IS_VERSION_20

if IS_VERSION_20:
    from sqlalchemy.orm import DeclarativeBase

    class SnowflakeBase(DeclarativeBase):
        """Declarative base that opts into :class:`SnowflakeSession` bulk tuning.

        Subclass this base for mapped classes that should use
        ``render_nulls=True`` during bulk INSERTs (see SQLAlchemy bulk caveats).
        For fewer INSERT batches, each row must expose the **same parameter
        keys** (use explicit ``None`` for unset optional columns rather than
        omitting attributes or mapping keys).

        Requires SQLAlchemy 2.0+. On SQLAlchemy 1.4, use :class:`SnowflakeBase`
        as an abstract mixin: ``class MyModel(SnowflakeBase, YourDeclarativeBase)``.
        """

        __abstract__ = True
        __snowflake_sqlalchemy_bulk__ = True

else:

    class SnowflakeBase:
        """Abstract mixin for SQLAlchemy 1.4 declarative models.

        Use multiple inheritance with your declarative base, mixin first:

        .. code-block:: python

            Base = declarative_base()

            class MyModel(SnowflakeBase, Base):
                __tablename__ = "my_model"
        """

        __abstract__ = True
        __snowflake_sqlalchemy_bulk__ = True


def mapper_uses_snowflake_bulk(mapper_or_class) -> bool:
    """Return True if the mapped class was declared with :class:`SnowflakeBase`."""
    mapper = _class_to_mapper(mapper_or_class)
    return bool(getattr(mapper.class_, "__snowflake_sqlalchemy_bulk__", False))


_RENDER_NULLS_AUTO = object()


class SnowflakeSession(Session):
    """Session that enables SQLAlchemy ``render_nulls`` for Snowflake-tuned mappers.

    For INSERT batches of instances whose class inherits :class:`SnowflakeBase`
    (or sets ``__snowflake_sqlalchemy_bulk__ = True``), bulk operations pass
    ``render_nulls=True`` so explicit ``None`` values are rendered as NULL and
    executemany batching works when every row already shares the same key set
    (see snowflake-sqlalchemy issue 441).

    :meth:`bulk_insert_mappings` defaults ``render_nulls`` to that same rule
    when the fourth argument is omitted (using an internal sentinel). Pass
    ``render_nulls=True`` or ``False`` explicitly to override.
    """

    def bulk_insert_mappings(
        self,
        mapper: Any,
        mappings: Iterable[dict[str, Any]],
        return_defaults: bool = False,
        render_nulls: Any = _RENDER_NULLS_AUTO,
    ) -> None:
        if render_nulls is _RENDER_NULLS_AUTO:
            render_nulls = mapper_uses_snowflake_bulk(mapper)
        super().bulk_insert_mappings(
            mapper,
            mappings,
            return_defaults=return_defaults,
            render_nulls=render_nulls,
        )

    def bulk_save_objects(
        self,
        objects: Iterable[object],
        return_defaults: bool = False,
        update_changed_only: bool = True,
        preserve_order: bool = True,
    ) -> None:
        obj_states = (attributes.instance_state(obj) for obj in objects)

        if not preserve_order:
            obj_states = sorted(
                obj_states,
                key=lambda state: (id(state.mapper), state.key is not None),
            )

        def grouping_key(state):
            return (state.mapper, state.key is not None)

        for (mapper, isupdate), states in itertools.groupby(obj_states, grouping_key):
            state_list = list(states)
            use_render_nulls = (not isupdate) and mapper_uses_snowflake_bulk(mapper)
            if IS_VERSION_20:
                self._bulk_save_mappings(
                    mapper,
                    state_list,
                    isupdate=isupdate,
                    isstates=True,
                    return_defaults=return_defaults,
                    update_changed_only=update_changed_only,
                    render_nulls=use_render_nulls,
                )
            else:
                self._bulk_save_mappings(
                    mapper,
                    state_list,
                    isupdate,
                    True,
                    return_defaults,
                    update_changed_only,
                    use_render_nulls,
                )
