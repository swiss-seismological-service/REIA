from typing import Optional

from sqlalchemy import select, true
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
    pass


class SiteRepository(repository_factory(
        Site, SiteORM)):
    pass


class AggregationTagRepository(repository_factory(
        AggregationTag, AggregationTagORM)):
    @classmethod
    def get_by_exposuremodel(
            cls,
            session: Session,
            exposuremodel_oid: int,
            type: Optional[str] = None,
            return_orm: bool = False) -> list[AggregationTag]:
        statement = select(cls.orm_model).where(
            ((cls.orm_model.type == type) if type is not None else true())
            & (cls.orm_model._exposuremodel_oid == exposuremodel_oid))
        result = session.execute(statement).unique().scalars().all()
        if return_orm:
            return result
        return [cls.model.model_validate(row) for row in result]


class AggregationGeometryRepository(repository_factory(
        AggregationGeometry, AggregationGeometryORM)):
    pass


class CostTypeRepository(repository_factory(
        CostType, CostTypeORM)):
    pass
