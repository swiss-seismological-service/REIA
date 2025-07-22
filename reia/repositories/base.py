from sqlalchemy import select
from sqlalchemy.orm import Session

from reia.datamodel.base import ORMBase
from reia.schemas.base import Model


def repository_factory(model: Model, orm_model: ORMBase):

    class RepositoryBase:
        model: Model
        orm_model: ORMBase

        @classmethod
        def create(cls, session: Session, data: Model) -> Model:
            db_model = cls.orm_model(**data.model_dump(exclude_unset=True))
            session.add(db_model)
            session.commit()
            session.refresh(db_model)
            return cls.model.model_validate(db_model)

        @classmethod
        def get_by_id(cls, session: Session, oid: int) -> Model:
            q = select(cls.orm_model).where(
                getattr(cls.orm_model, '_oid') == oid)
            result = session.execute(q).unique().scalar_one_or_none()
            return cls.model.model_validate(result) if result else None

        @classmethod
        def update(cls, session: Session, data: Model) -> Model:
            q = select(cls.orm_model).where(
                getattr(cls.orm_model, '_oid') == data.oid)
            result = session.execute(q).unique().scalar_one_or_none()
            if result:
                for key, value in data.model_dump(exclude_unset=True).items():
                    setattr(result, key, value)
                session.commit()
                session.refresh(result)
                return cls.model.model_validate(result)
            else:
                raise ValueError(f'No object with id {data.oid} found')

        @classmethod
        def get_all(cls, session: Session) -> list:
            q = select(cls.orm_model).order_by(getattr(cls.orm_model, '_oid'))
            result = session.execute(q).unique().scalars().all()
            return [cls.model.model_validate(row) for row in result]

        @classmethod
        def delete(cls, session: Session, oid: int) -> None:
            q = select(cls.orm_model).where(
                getattr(cls.orm_model, '_oid') == oid)
            result = session.execute(q).unique().scalar_one_or_none()
            if result:
                session.delete(result)
                session.commit()
            else:
                raise ValueError(f'No object with id {oid} found')

    RepositoryBase.model = model
    RepositoryBase.orm_model = orm_model

    return RepositoryBase
