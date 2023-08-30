from asyncpg import Record
from dataclasses import dataclass, is_dataclass
from typing import TypeVar


DC = TypeVar('DC')


@dataclass
class Entity:
    id: int
    name: str


def _map_record(entity: type[DC], db_record: Record) -> DC:
    d = dict(db_record.items())
    res = entity(**d)
    return res


def map_entity(entity: type[DC], db_records: Record | list[Record]) -> DC | list[DC]:
    if not is_dataclass(entity):
        raise Exception
    
    if not isinstance(db_records, list):
        return _map_record(entity, db_records)

    entities = []
    for db_record in db_records:
        entities.append(_map_record(entity, db_record))
    
    return entities


