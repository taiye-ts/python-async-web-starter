import pickle
import re
from typing import TypeVar, Optional, Generic, List, Dict, Callable, Any, Union, Match

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2, PGCompiler_psycopg2
from sqlalchemy.engine import RowProxy
from sqlalchemy.sql.expression import select, Select, delete, Delete, bindparam, insert, Insert, Update, update

T_ID = TypeVar('T_ID')  # pylint: disable=invalid-name
T = TypeVar('T')  # pylint: disable=invalid-name

FuncType = Callable[..., Any]
F = TypeVar('F', bound=FuncType)  # pylint: disable=invalid-name


class CommonQueryBuilderMixin:
    @property
    def table(self) -> Table:
        raise NotImplementedError()

    @property
    def id_query(self):
        return self.table.c.id == bindparam('instance_id')

    def get_by_id_query(self) -> Select:
        return self.get_all_query().where(self.id_query)

    def get_by_id_for_update_query(self) -> Select:
        return self.get_all_query().with_for_update().where(self.id_query)

    def delete_by_id_query(self) -> Delete:
        return delete(self.table).where(self.id_query)

    def get_all_query(self) -> Select:
        return select([self.table])

    def insert_query(self) -> Insert:
        return insert(self.table)

    def update_query(self) -> Update:
        return update(self.table).where(self.id_query)

    def delete_all_query(self) -> Delete:
        return delete(self.table)


class CommonSerializerMixin(Generic[T, T_ID]):

    def get_instance_id(self, instance: T) -> T_ID:
        raise NotImplementedError()

    def get_instances(self, records: List[RowProxy]) -> List[T]:
        return list(map(self.get_instance, records))

    def get_instance(self, record: RowProxy) -> T:
        raise NotImplementedError()

    def instance_to_dict(self, instance: T) -> Dict:
        serializer = getattr(instance, 'to_dict', None)
        if callable(serializer):
            return serializer()
        raise NotImplementedError()

    def _deserialize(self, result: Optional[bytes]) -> Optional[Union[List[T], T]]:
        if isinstance(result, bytes):
            return pickle.loads(result)
        return None

    def _serialize(self, instance: T) -> bytes:
        return pickle.dumps(instance, protocol=4)

    def instance_id_as_dict(self, instance_id: T_ID) -> Dict[str, Any]:
        """Use this function if table has multicolumn primary key.
        See RegistrationRepository for example."""
        return {'instance_id': instance_id}


class AsyncPGCompiler(PGCompiler_psycopg2):
    """Custom SA PostgreSQL compiler that produces param placeholder
    compatible with asyncpg.

    This solves https://github.com/MagicStack/asyncpg/issues/32.
    """

    def _apply_numbered_params(self) -> None:
        idx = 0

        def replace(match: Match[Any]) -> Any:
            nonlocal idx
            idx += 1
            return str(idx)

        self.string = re.sub(r'\[_POSITION\]', replace, self.string)  # type: str


class AsyncPGDialect(PGDialect_psycopg2):
    """Custom SA PostgreSQL dialect compatible with asyncpg.

    In particular it uses a variant of the ``numeric`` `paramstyle`, to
    produce placeholders like ``$1``, ``$2`` and so on.
    """

    statement_compiler = AsyncPGCompiler

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs['paramstyle'] = 'numeric'
        super().__init__(*args, **kwargs)
        self.implicit_returning = True
        self.supports_native_enum = True
        self.supports_smallserial = True
        self._backslash_escapes = False
        self.supports_sane_multi_rowcount = True
        self._has_native_hstore = True
        self._has_native_json = True
        self._has_native_jsonb = True
