from typing import Optional

import pandas as pd
from sqlalchemy import select, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from reia.datamodel.asset import AggregationGeometry as AggregationGeometryORM
from reia.datamodel.asset import AggregationTag as AggregationTagORM
from reia.datamodel.asset import Asset as AssetORM
from reia.datamodel.asset import CostType as CostTypeORM
from reia.datamodel.asset import ExposureModel as ExposureModelORM
from reia.datamodel.asset import Site as SiteORM
from reia.repositories.base import repository_factory
from reia.schemas.asset_schemas import (AggregationGeometry, AggregationTag,
                                        Asset, CostType, ExposureModel, Site)


class ExposureModelRepository(repository_factory(
        ExposureModel, ExposureModelORM)):
    pass


class AssetRepository(repository_factory(
        Asset, AssetORM)):
    @classmethod
    def insert_many(cls, session: Session, assets: pd.DataFrame) -> list[int]:
        stmt = insert(AssetORM).values(
            assets.to_dict(orient='records')).returning(AssetORM._oid)
        result = session.execute(stmt)
        return [row._oid for row in result]


class SiteRepository(repository_factory(
        Site, SiteORM)):
    @classmethod
    def insert_many(
            cls, session: Session, sites: pd.DataFrame) -> list[int]:
        stmt = insert(SiteORM).values(
            sites.to_dict(orient='records')).returning(SiteORM._oid)
        result = session.execute(stmt)
        return [row._oid for row in result]


class AggregationTagRepository(repository_factory(
        AggregationTag, AggregationTagORM)):
    @classmethod
    def get_by_exposuremodel(cls,
                             session: Session,
                             exposuremodel_oid: int,
                             type: Optional[str] = None) \
            -> list[AggregationTag]:

        stmt = select(cls.orm_model).where(
            ((cls.orm_model.type == type) if type is not None else true())
            & (cls.orm_model._exposuremodel_oid == exposuremodel_oid))
        result = session.execute(stmt).unique().scalars().all()
        return [cls.model.model_validate(row) for row in result]

    @classmethod
    def insert_many(cls, session: Session,
                    aggregationtags: pd.DataFrame) -> list[int]:

        stmt = insert(AggregationTagORM) \
            .values(aggregationtags.to_dict(orient='records')) \
            .on_conflict_do_update(
                index_elements=['name', 'type', '_exposuremodel_oid'],
                set_={'name': AggregationTagORM.name}) \
            .returning(AggregationTagORM._oid)
        result = session.execute(stmt)
        return [row._oid for row in result]


class AggregationGeometryRepository(repository_factory(
        AggregationGeometry, AggregationGeometryORM)):
    pass


class CostTypeRepository(repository_factory(
        CostType, CostTypeORM)):
    pass
