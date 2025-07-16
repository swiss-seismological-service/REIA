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
    pass


class AggregationGeometryRepository(repository_factory(
        AggregationGeometry, AggregationGeometryORM)):
    pass


class CostTypeRepository(repository_factory(
        CostType, CostTypeORM)):
    pass
