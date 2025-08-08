import numpy as np
import pandas as pd
from sqlalchemy import case, delete, func, select, text, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from reia.datamodel.asset import AggregationGeometry as AggregationGeometryORM
from reia.datamodel.asset import AggregationTag as AggregationTagORM
from reia.datamodel.asset import Asset as AssetORM
from reia.datamodel.asset import Site as SiteORM
from reia.datamodel.asset import asset_aggregationtag
from reia.datamodel.exposure import CostType as CostTypeORM
from reia.datamodel.exposure import ExposureModel as ExposureModelORM
from reia.repositories import pandas_read_sql
from reia.repositories.base import repository_factory
from reia.repositories.utils import (allocate_oids, copy_pooled,
                                     db_cursor_from_session)
from reia.schemas.asset_schemas import (AggregationGeometry, AggregationTag,
                                        Asset, Site)
from reia.schemas.exposure_schema import CostType, ExposureModel


class ExposureModelRepository(repository_factory(
        ExposureModel, ExposureModelORM)):
    @classmethod
    def create(cls, session: Session, data: ExposureModel) -> ExposureModel:

        cost_types = [
            CostTypeORM(**ct.model_dump(exclude_unset=True))
            for ct in data.costtypes
        ]

        db_model = ExposureModelORM(
            **data.model_dump(exclude_unset=True, exclude=('costtypes',)))
        db_model.costtypes = cost_types

        session.add(db_model)
        session.commit()
        session.refresh(db_model)
        return cls.model.model_validate(db_model)


class AssetRepository(repository_factory(
        Asset, AssetORM)):
    @classmethod
    def get_by_exposuremodel(cls, session: Session,
                             exposuremodel_oid: int) -> pd.DataFrame:

        # Get distinct aggregation tag types
        tagtype_query = select(AggregationTagORM.type).distinct().where(
            AggregationTagORM._exposuremodel_oid == exposuremodel_oid
        )
        result = session.execute(tagtype_query)
        tagtypes = [row[0] for row in result]

        # Build dynamic pivot columns for aggregation tags
        pivot_columns = [
            func.max(case((AggregationTagORM.type == tagtype,
                           AggregationTagORM.name))).label(tagtype)
            for tagtype in tagtypes
        ]

        asset_cols = [getattr(AssetORM, col.name)
                      for col in AssetORM.__table__.columns]

        stmt = select(*asset_cols,
                      SiteORM.longitude,
                      SiteORM.latitude,
                      *pivot_columns)\
            .join(SiteORM, AssetORM._site_oid == SiteORM._oid) \
            .join(asset_aggregationtag,
                  AssetORM._oid == asset_aggregationtag.c.asset) \
            .join(AggregationTagORM,
                  (asset_aggregationtag.c.aggregationtag
                   == AggregationTagORM._oid)
                  & (asset_aggregationtag.c.aggregationtype
                     == AggregationTagORM.type)) \
            .where(AssetORM._exposuremodel_oid == exposuremodel_oid) \
            .group_by(AssetORM._oid,
                      SiteORM.longitude,
                      SiteORM.latitude,)

        return pandas_read_sql(stmt, session)

    @classmethod
    def insert_many(cls, session: Session, assets: pd.DataFrame) -> list[int]:
        stmt = insert(AssetORM).values(
            assets.to_dict(orient='records')).returning(AssetORM._oid)
        result = session.execute(stmt)
        return [row._oid for row in result]

    @classmethod
    def insert_many_bulk(cls, session: Session,
                         assets: pd.DataFrame) -> list[int]:
        """Bulk insert assets using COPY and pre-allocated OIDs.

        Args:
            session: SQLAlchemy session.
            assets: DataFrame containing asset data.

        Returns:
            List of OIDs of the inserted assets in the same order as input.
        """
        with db_cursor_from_session(session) as cursor:
            db_indexes = allocate_oids(cursor,
                                       AssetORM.__table__.name,
                                       '_oid',
                                       len(assets))

        # Create a copy and assign pre-allocated OIDs
        assets_copy = assets.copy()
        assets_copy['_oid'] = db_indexes

        # Use bulk copy for insertion
        copy_pooled(assets_copy, AssetORM.__table__.name)

        return db_indexes

    @classmethod
    def insert_from_exposuremodel(cls,
                                  session: Session,
                                  sites: pd.DataFrame,
                                  assets: pd.DataFrame,
                                  aggregationtags: pd.DataFrame,
                                  assoc_assets_tags: pd.DataFrame
                                  ) -> tuple[list[int], list[int]]:
        """Insert assets and their associated tags into the database.
        Args:
            session: SQLAlchemy session.
            sites: DataFrame containing site data.
            assets: DataFrame containing asset data.
            aggregationtags: DataFrame containing aggregation tags.
            assoc_assets_tags: DataFrame mapping assets to tags.

        Returns:
            List of OIDs of the inserted assets and sites.
        """

        # Use bulk insert for sites with pre-allocated OIDs
        sites_oids = SiteRepository.insert_many_bulk(session, sites.copy())

        # Update assets with site OIDs using index-based mapping
        assets_copy = assets.copy()
        site_oid_map = dict(zip(range(len(sites)), sites_oids))
        assets_copy['_site_oid'] = assets_copy['_site_oid'].map(site_oid_map)

        # Keep using SQLAlchemy approach for aggregation tags
        # (needed for conflict resolution)
        tags_oids = AggregationTagRepository.insert_many(
            session, aggregationtags)

        # Update associations with tag OIDs using index-based mapping
        assoc_copy = assoc_assets_tags.copy()
        tag_oid_map = dict(zip(range(len(aggregationtags)), tags_oids))
        assoc_copy['aggregationtag'] = \
            assoc_copy['aggregationtag'].map(tag_oid_map)

        # Use bulk insert for assets with pre-allocated OIDs
        assets_oids = AssetRepository.insert_many_bulk(session, assets_copy)

        # Update associations with asset OIDs using index-based mapping
        asset_oid_map = dict(zip(range(len(assets)), assets_oids))
        assoc_copy['asset'] = assoc_copy['asset'].map(asset_oid_map)

        # Use bulk insert for associations
        AssetAggregationTagRepository.insert_many(session, assoc_copy)

        # Refresh materialized views after asset insertion
        session.execute(
            text('REFRESH MATERIALIZED VIEW CONCURRENTLY '
                 'loss_buildings_per_municipality'))
        session.commit()

        return assets_oids, sites_oids

    @classmethod
    def count_by_exposuremodel(cls, session: Session,
                               exposuremodel_oid: int) -> int:
        """Count the number of assets associated with an exposure model."""
        stmt = select(func.count(AssetORM._oid)).where(
            AssetORM._exposuremodel_oid == exposuremodel_oid)
        result = session.execute(stmt).scalar_one()
        session.commit()
        return result


class SiteRepository(repository_factory(
        Site, SiteORM)):
    @classmethod
    def insert_many(
            cls, session: Session, sites: pd.DataFrame) -> list[int]:
        stmt = insert(SiteORM).values(
            sites.to_dict(orient='records')).returning(SiteORM._oid)
        result = session.execute(stmt)
        session.commit()
        return [row._oid for row in result]

    @classmethod
    def insert_many_bulk(cls, session: Session,
                         sites: pd.DataFrame) -> list[int]:
        """Bulk insert sites using COPY and pre-allocated OIDs.

        Args:
            session: SQLAlchemy session.
            sites: DataFrame containing site data.

        Returns:
            List of OIDs of the inserted sites in the same order as input.
        """
        with db_cursor_from_session(session) as cursor:
            db_indexes = allocate_oids(cursor,
                                       SiteORM.__table__.name,
                                       '_oid',
                                       len(sites))

        # Create a copy and assign pre-allocated OIDs
        sites_copy = sites.copy()
        sites_copy['_oid'] = db_indexes

        # Use bulk copy for insertion
        copy_pooled(sites_copy, SiteORM.__table__.name)

        return db_indexes

    @classmethod
    def get_by_exposuremodel(cls, session: Session,
                             exposuremodel_oid: int) -> list[Site]:
        stmt = select(cls.orm_model).where(
            cls.orm_model._exposuremodel_oid == exposuremodel_oid)
        result = session.execute(stmt).unique().scalars().all()
        session.commit()
        return [cls.model.model_validate(row) for row in result]

    @classmethod
    def count_by_exposuremodel(cls, session: Session,
                               exposuremodel_oid: int) -> int:
        stmt = select(func.count(SiteORM._oid)).where(
            SiteORM._exposuremodel_oid == exposuremodel_oid)
        result = session.execute(stmt).scalar_one()
        session.commit()
        return result


class AggregationTagRepository(repository_factory(
        AggregationTag, AggregationTagORM)):
    @classmethod
    def get_by_exposuremodel(cls,
                             session: Session,
                             exposuremodel_oid: int,
                             types: list[str] | None = None) \
            -> list[AggregationTag]:

        stmt = select(AggregationTagORM) \
            .where(AggregationTagORM._exposuremodel_oid == exposuremodel_oid) \
            .where(AggregationTagORM.type.in_(types) if types else true())

        result = session.execute(stmt).unique().scalars().all()
        session.commit()
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
        session.commit()
        return [row._oid for row in result]


class AggregationGeometryRepository(repository_factory(
        AggregationGeometry, AggregationGeometryORM)):
    @classmethod
    def insert_many(cls,
                    session: Session,
                    exposuremodel_oid: int,
                    geometries: pd.DataFrame) -> list[int]:
        """Insert multiple aggregation geometries into the database.
        Args:
            session: SQLAlchemy session.
            exposuremodel_oid: OID of the exposure model.
            geometries: DataFrame containing aggregation geometries.
        Returns:
            List of OIDs of the inserted aggregation geometries.
        """
        geometries['_exposuremodel_oid'] = exposuremodel_oid

        aggregationtags = AggregationTagRepository.get_by_exposuremodel(
            session,
            exposuremodel_oid,
            geometries['_aggregationtype'].unique().tolist())

        lookup = {tag.name: tag.oid for tag in aggregationtags}

        geometries['_aggregationtag_oid'] = geometries['aggregationtag'] \
            .map(lookup) \
            .replace({np.NAN: None})

        geometries.drop(columns=['aggregationtag'], inplace=True)

        stmt = insert(AggregationGeometryORM) \
            .values(geometries.to_dict(orient='records')) \
            .returning(AggregationGeometryORM._oid)

        result = session.execute(stmt)
        session.commit()
        return [row._oid for row in result]

    @classmethod
    def delete_by_exposuremodel(cls,
                                session: Session,
                                exposuremodel_oid: int,
                                aggregationtype: str | None) -> None:
        """Delete all aggregation geometries associated with an exposure model.
        """
        stmt = delete(AggregationGeometryORM) \
            .where(AggregationGeometryORM._exposuremodel_oid
                   == exposuremodel_oid) \
            .where(AggregationGeometryORM._aggregationtype
                   == aggregationtype if aggregationtype else true())
        session.execute(stmt)
        session.commit()

    @classmethod
    def get_by_exposuremodel(cls,
                             session: Session,
                             exposuremodel_oid: int,
                             aggregationtype: str | None = None) \
            -> list[AggregationGeometry]:
        """Get aggregation geometries by exposure model OID."""
        stmt = select(AggregationGeometryORM) \
            .where(AggregationGeometryORM._exposuremodel_oid
                   == exposuremodel_oid) \
            .where(AggregationGeometryORM._aggregationtype
                   == aggregationtype if aggregationtype else true())
        result = session.execute(stmt).unique().scalars().all()
        session.commit()
        return [cls.model.model_validate(row) for row in result]


class CostTypeRepository(repository_factory(
        CostType, CostTypeORM)):
    pass


class AssetAggregationTagRepository:
    """Repository for managing asset-aggregationtag associations."""

    @classmethod
    def insert_many(cls, session: Session, associations: pd.DataFrame) -> None:
        """Insert multiple asset-aggregationtag associations using bulk copy.

        Args:
            session: SQLAlchemy session.
            associations: DataFrame with columns: asset, aggregationtag,
                         aggregationtype.
        """
        copy_pooled(associations, asset_aggregationtag.name)
