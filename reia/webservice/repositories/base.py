from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from reia.webservice.database import Base as ORMBase
from reia.webservice.schemas import WSModel


def async_repository_factory(model: WSModel, orm_model: ORMBase):

    class AsyncRepositoryBase:
        model: WSModel
        orm_model: ORMBase

        @classmethod
        async def get_by_id(cls, session: AsyncSession, oid: str | UUID
                            ) -> WSModel:
            q = select(cls.orm_model).where(
                getattr(cls.orm_model, 'oid') == oid)
            result = await session.execute(q)
            result = result.unique().scalar_one_or_none()
            return cls.model.model_validate(result) if result else None

        @classmethod
        async def get_all(cls, session: AsyncSession) -> list:
            q = select(cls.orm_model)
            result = await session.execute(q)
            result = result.unique().scalars().all()
            return [cls.model.model_validate(row) for row in result]

    AsyncRepositoryBase.model = model
    AsyncRepositoryBase.orm_model = orm_model

    return AsyncRepositoryBase
