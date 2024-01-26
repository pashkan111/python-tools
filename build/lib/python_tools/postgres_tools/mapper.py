from asyncpg import Record
from typing import TypeVar


T = TypeVar("T")


def _map_record(entity: type[T], db_record: Record) -> T:
    d = dict(db_record.items())
    res = entity(**d)
    return res


def map_entity(entity: type[T], db_records: Record | list[Record]) -> T | list[T]:
    if not isinstance(db_records, list):
        return _map_record(entity, db_records)

    entities = []
    for db_record in db_records:
        entities.append(_map_record(entity, db_record))

    return entities
