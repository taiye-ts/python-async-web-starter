from typing import Optional, Generic, List, TypeVar, Dict, Any, Callable, Tuple

from asyncpg.protocol import protocol

from project_name.exceptions import NotFoundInRepository
from project_name.storage.database import db
from project_name.storage.database.base import CommonQueryBuilderMixin, CommonSerializerMixin, AsyncPGDialect

T_ID = TypeVar('T_ID')  # pylint: disable=invalid-name
T = TypeVar('T')  # pylint: disable=invalid-name


class BaseAsyncRepository(Generic[T, T_ID], CommonQueryBuilderMixin, CommonSerializerMixin[T, T_ID]):
    not_found_exception_cls = NotFoundInRepository

    def __init__(self):
        super(BaseAsyncRepository, self).__init__()
        self._queries: Dict[str, Any] = {}
        self.name = self.__class__.__name__

    async def _fetchone(self, query: str, *args, timeout=None) -> protocol.Record:
        pool = await db.get_pool()
        async with pool.acquire() as con:
            result = await con.fetchrow(query, *args, timeout=timeout)
        return result

    async def _fetchall(self, query: str, *args, timeout=None) -> List[protocol.Record]:
        pool = await db.get_pool()
        async with pool.acquire() as con:
            result = await con.fetch(query, *args, timeout=timeout)
        return result

    async def _execute(self, query: str, *args, timeout=None) -> str:
        pool = await db.get_pool()
        async with pool.acquire() as con:
            result = await con.execute(query, *args, timeout=timeout)
        return result

    def _get_row_count(self, db_response: str) -> int:
        return int(db_response.split(' ')[-1])

    def _get_query_and_args(
        self,
        query_builder_func: Callable,
        query_params: dict,
        cache_query: bool = False,
        query_builder_args: List = None,
        query_builder_kwargs: Dict = None,
    ) -> Tuple[str, List[str]]:
        query_name = query_builder_func.__name__

        compiled_query = self._queries.get(query_name, None) if cache_query else None
        if compiled_query is None:
            query_builder_args = query_builder_args or []
            query_builder_kwargs = query_builder_kwargs or {}
            query = query_builder_func(*query_builder_args, **query_builder_kwargs)
            compiled_query = query.compile(dialect=AsyncPGDialect())
            self._queries[query_name] = compiled_query

        params = compiled_query.construct_params(query_params)
        params = tuple(params[p] for p in compiled_query.positiontup)
        string_query = compiled_query.string

        return string_query, params

    async def get_by_id(self, instance_id: T_ID) -> Optional[T]:
        query, args = self._get_query_and_args(
            self.get_by_id_query, self.instance_id_as_dict(instance_id), cache_query=True
        )
        result = await self._fetchone(query, *args)
        if result is None:
            return None
        return self.get_instance(result)

    async def get_or_raise_by_id(self, instance_id: T_ID) -> T:
        result = await self.get_by_id(instance_id)
        if result is None:
            raise self.not_found_exception_cls()
        return result

    async def update(self, instance: T) -> int:
        params = self.instance_to_dict(instance)
        if 'id' in params:
            params['instance_id'] = params['id']

        query, args = self._get_query_and_args(self.update_query, params, cache_query=True)
        result = await self._execute(query, *args)

        return self._get_row_count(result)

    async def insert(self, instance: T) -> int:
        params = self.instance_to_dict(instance)

        query, args = self._get_query_and_args(self.insert_query, params, cache_query=True)
        result = await self._execute(query, *args)

        return self._get_row_count(result)

    async def delete_all(self):
        query, args = self._get_query_and_args(self.delete_all_query, {}, cache_query=True)
        return await self._execute(query, *args)

    def get_instance(self, record):
        result = super(BaseAsyncRepository, self).get_instance(record)
        return result
