from collections.abc import Sequence
from typing import Any, cast

from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.postgresql.base import PGCompiler
from sqlalchemy.sql.dml import Insert as InsertObject, Update as UpdateObject
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.util import immutabledict


_dialect: PGDialect_asyncpg = postgresql.asyncpg.dialect()


def _get_default_values(statement: ClauseElement, compiled: PGCompiler) -> dict[str, Any]:
    """
    Возвращает дефолтные значения для параметров запроса вставки/обновления
    """
    if isinstance(statement, InsertObject):
        attr_name = "default"
    elif isinstance(statement, UpdateObject):
        attr_name = "onupdate"
    else:
        # для других запросов дефолтные значения не применимы
        return {}

    # Переданные аргументы в statement
    _values = cast(immutabledict, statement._values)
    statement_values: frozenset[str] = frozenset(_values.keys()) if _values else frozenset()

    params: dict[str, Any] = {}

    for col in statement.table.columns:
        if col.name in statement_values:
            # Если значение передано в values, даже если оно null
            # не берем дефолтные значения
            continue

        # дефолтное значение
        attr = getattr(col, attr_name)

        if attr and compiled.params.get(col.name) is None:
            # Если определено дефолтное значение
            if attr.is_sequence:
                params[col.name] = func.nextval(attr.name)
            elif attr.is_scalar:
                params[col.name] = attr.arg
            elif attr.is_callable:
                params[col.name] = attr.arg({})

    return params


def _get_compiled_statement(statement: ClauseElement) -> PGCompiler:
    return cast(
        PGCompiler,  # На деле возвращается PGCompiler_asyncpg
        statement.compile(
            dialect=_dialect,
            compile_kwargs={
                "render_postcompile": True,
            },
        ),
    )


def compile_query(statement: ClauseElement) -> tuple[str, tuple[Any, ...]]:
    compiled: PGCompiler = _get_compiled_statement(statement)

    positiontup: Sequence[str] | None = compiled.positiontup

    if not positiontup:
        return compiled.string, ()

    default_values = _get_default_values(statement, compiled)

    values = {
        **compiled.params,
        **default_values,
    }

    params = tuple(values[param] for param in positiontup)

    return compiled.string, params
