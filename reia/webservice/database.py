import contextlib
from typing import Annotated, Any, AsyncIterator

from fastapi import Depends
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import (AsyncAttrs, AsyncConnection, AsyncSession,
                                    async_sessionmaker, create_async_engine)
from sqlalchemy.orm import DeclarativeBase

from reia.config.settings import get_webservice_settings


class Base(AsyncAttrs, DeclarativeBase):
    pass


class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        self._engine = create_async_engine(
            host, pool_size=10, max_overflow=5, **engine_kwargs)
        self._sessionmaker = async_sessionmaker(
            autoflush=False, bind=self._engine)

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


sessionmanager = DatabaseSessionManager(
    get_webservice_settings().SQLALCHEMY_DATABASE_URL, {"echo": False})


async def get_db():
    async with sessionmanager.session() as session:
        yield session

DBSessionDep = Annotated[AsyncSession, Depends(get_db)]


async def paginate(
        session: AsyncSession, query: Select, limit: int, offset: int) -> dict:
    return {
        'count': await session.scalar(select(func.count())
                                      .select_from(query.subquery())),
        'items': [todo for todo in await session.scalars(query.limit(limit)
                                                         .offset(offset))]
    }
