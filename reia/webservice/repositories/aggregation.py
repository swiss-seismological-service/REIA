import pandas as pd
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from reia.datamodel import (AggregationTag, Asset, Calculation,
                            CalculationBranch, DamageCalculationBranch,
                            DamageValue, ExposureModel, LossValue,
                            asset_aggregationtag, riskvalue_aggregationtag)
from reia.schemas.enums import ECalculationType, ELossCategory
from reia.webservice.schemas import WSRiskCategory
from reia.webservice.utils import pandas_read_sql


class AggregationRepository:
    """Repository for aggregation-related queries and statistics"""

    @staticmethod
    def losscategory_filter(f, model):
        return model.losscategory == f if f else True

    @staticmethod
    def tagname_filter(f, model):
        return model.aggregationtags.any(
            AggregationTag.name == f) if f else True

    @staticmethod
    def tagname_like_filter(f, model):
        return model.aggregationtags.any(
            AggregationTag.name.like(f)) if f else True

    @staticmethod
    def tagtype_filter(t, model):
        return model.aggregationtags.any(
            AggregationTag.type == t) if t else True

    @staticmethod
    def calculationid_filter(f, model):
        return model._calculation_oid == f if f else True

    @staticmethod
    def aggregation_type_subquery(aggregation_type):
        return select(AggregationTag).where(
            AggregationTag.type == aggregation_type).subquery()

    @classmethod
    async def get_aggregation_types(cls, session: AsyncSession) -> dict:
        """Get all distinct aggregation types"""
        stmt = select(AggregationTag.type).distinct()
        results = await session.execute(stmt)
        types = results.scalars().all()
        edict = {}
        for t in types:
            edict[t.upper()] = t
        return edict

    @classmethod
    async def get_total_buildings_country(
        cls, session: AsyncSession, calculation_id: int
    ) -> int | None:
        """Get total building count for a calculation across country"""
        exp_sub = select(ExposureModel._oid) \
            .join(DamageCalculationBranch) \
            .join(Calculation) \
            .where(Calculation._oid == calculation_id) \
            .limit(1).scalar_subquery()

        stmt = select(func.sum(Asset.buildingcount).label('buildingcount')) \
            .select_from(Asset) \
            .where(Asset._exposuremodel_oid == exp_sub)
        result = await session.execute(stmt)
        return result.scalar()

    @classmethod
    async def get_total_buildings(
        cls,
        session: AsyncSession,
        calculation_id: int,
        aggregation_type: str,
        filter_tag: str | None = None,
        filter_like_tag: str | None = None
    ) -> pd.DataFrame:
        """Get total buildings aggregated by type with optional filtering"""

        type_sub = cls.aggregation_type_subquery(aggregation_type)

        exp_sub = select(ExposureModel._oid) \
            .join(DamageCalculationBranch) \
            .join(Calculation) \
            .where(Calculation._oid == calculation_id) \
            .limit(1).subquery()

        filter_condition = cls.tagname_like_filter(filter_like_tag, Asset)
        filter_condition &= cls.tagname_filter(filter_tag, Asset)

        stmt = select(func.sum(Asset.buildingcount).label('buildingcount'),
                      type_sub.c.name.label(aggregation_type))\
            .select_from(Asset)\
            .join(asset_aggregationtag) \
            .join(type_sub) \
            .join(exp_sub, exp_sub.c._oid == Asset._exposuremodel_oid) \
            .where(filter_condition) \
            .group_by(type_sub.c.name)

        return await pandas_read_sql(stmt, session)

    @classmethod
    async def get_aggregated_loss(
        cls,
        session: AsyncSession,
        calculation_id: int,
        aggregation_type: str,
        loss_category: WSRiskCategory,
        filter_tag: str | None = None,
        filter_like_tag: str | None = None
    ) -> pd.DataFrame:
        """Get aggregated loss data with filtering"""

        loss_category = ELossCategory[loss_category.name]

        risk_sub = select(LossValue).where(and_(
            LossValue._calculation_oid == calculation_id,
            LossValue.losscategory == loss_category,
            LossValue._type == ECalculationType.LOSS
        )).subquery()

        agg_sub = select(AggregationTag).where(and_(
            AggregationTag.type == aggregation_type,
            AggregationTag.name.like(
                filter_like_tag) if filter_like_tag else True,
            (AggregationTag.name == filter_tag) if filter_tag else True
        )).subquery()

        stmt = select(risk_sub.c.loss_value,
                      risk_sub.c.weight,
                      risk_sub.c._calculationbranch_oid.label('branchid'),
                      risk_sub.c.eventid,
                      agg_sub.c.name.label(aggregation_type)) \
            .select_from(risk_sub) \
            .join(riskvalue_aggregationtag, and_(
                riskvalue_aggregationtag.c.riskvalue == risk_sub.c._oid,
                riskvalue_aggregationtag.c.losscategory
                == risk_sub.c.losscategory,
                riskvalue_aggregationtag.c._calculation_oid
                == risk_sub.c._calculation_oid
            )) \
            .join(agg_sub, and_(
                agg_sub.c._oid == riskvalue_aggregationtag.c.aggregationtag,
                agg_sub.c.type == riskvalue_aggregationtag.c.aggregationtype
            )) \
            .where(and_(
                agg_sub.c.name.like(
                    filter_like_tag) if filter_like_tag else True,
                (AggregationTag.name == filter_tag) if filter_tag else True,
                risk_sub.c.losscategory == loss_category,
                risk_sub.c._calculation_oid == calculation_id,
                risk_sub.c._type == ECalculationType.LOSS
            ))
        return await pandas_read_sql(stmt, session)

    @classmethod
    async def get_aggregation_tags(
        cls,
        session: AsyncSession,
        aggregation_type: str,
        calculation_id: int,
        tag_like: str | None
    ) -> pd.DataFrame:
        """Get aggregation tags for a calculation with optional filtering"""

        stmt = select(ExposureModel._oid) \
            .join(CalculationBranch) \
            .join(Calculation) \
            .where(Calculation._oid == calculation_id)

        exposuremodel_oids = await session.execute(stmt)
        exposuremodel_oids = exposuremodel_oids.unique().scalars().all()

        stmt = select(AggregationTag.name.label(aggregation_type)).where(and_(
            AggregationTag.type == aggregation_type,
            AggregationTag.name.like(tag_like) if tag_like else True,
            AggregationTag._exposuremodel_oid.in_(exposuremodel_oids)
        ))

        df = await pandas_read_sql(stmt, session)
        df.drop_duplicates(inplace=True)
        return df

    @classmethod
    async def get_aggregated_damage(
        cls,
        session: AsyncSession,
        calculation_id: int,
        aggregation_type: str,
        loss_category: WSRiskCategory,
        filter_tag: str | None = None,
        filter_like_tag: str | None = None
    ) -> pd.DataFrame:
        """Get aggregated damage data with filtering"""

        loss_category = ELossCategory[loss_category.name]

        damage_sub = select(DamageValue).where(and_(
            DamageValue._calculation_oid == calculation_id,
            DamageValue.losscategory == loss_category,
            DamageValue._type == ECalculationType.DAMAGE
        )).subquery()

        agg_sub = select(
            AggregationTag.name,
            AggregationTag.type,
            AggregationTag._oid
        ).where(and_(
                AggregationTag.type == aggregation_type,
                AggregationTag.name.like(
                    filter_like_tag) if filter_like_tag else True,
                (AggregationTag.name == filter_tag) if filter_tag else True
                )).subquery()

        stmt = select(damage_sub.c.dg1_value.label('dg1_value'),
                      damage_sub.c.dg2_value.label('dg2_value'),
                      damage_sub.c.dg3_value.label('dg3_value'),
                      damage_sub.c.dg4_value.label('dg4_value'),
                      damage_sub.c.dg5_value.label('dg5_value'),
                      damage_sub.c.weight,
                      damage_sub.c._calculationbranch_oid.label('branchid'),
                      damage_sub.c.eventid,
                      agg_sub.c.name.label(aggregation_type)) \
            .select_from(damage_sub) \
            .join(riskvalue_aggregationtag, and_(
                riskvalue_aggregationtag.c.riskvalue == damage_sub.c._oid,
                riskvalue_aggregationtag.c.losscategory
                == damage_sub.c.losscategory,
                riskvalue_aggregationtag.c._calculation_oid
                == damage_sub.c._calculation_oid
            )) \
            .join(agg_sub, and_(
                agg_sub.c._oid == riskvalue_aggregationtag.c.aggregationtag,
                agg_sub.c.type == riskvalue_aggregationtag.c.aggregationtype
            )) \
            .where(and_(
                agg_sub.c.name.like(
                    filter_like_tag) if filter_like_tag else True,
                (AggregationTag.name == filter_tag) if filter_tag else True,
                damage_sub.c.losscategory == loss_category,
                damage_sub.c._calculation_oid == calculation_id,
                damage_sub.c._type == ECalculationType.DAMAGE
            ))

        return await pandas_read_sql(stmt, session)

    @classmethod
    async def get_mean_losses(
        cls,
        session: AsyncSession,
        calculation_id,
        aggregation_type,
        loss_category,
        filter_tag: str | None = None,
        filter_like_tag: str | None = None
    ) -> pd.DataFrame:
        """Get mean losses aggregated by type"""

        filter_condition = cls.calculationid_filter(calculation_id, LossValue)
        filter_condition &= cls.losscategory_filter(loss_category, LossValue)
        filter_condition &= cls.tagname_filter(filter_tag, LossValue)
        filter_condition &= cls.tagname_like_filter(filter_like_tag, LossValue)

        type_sub = cls.aggregation_type_subquery(aggregation_type)

        stmt = select(func.sum(LossValue.loss_value * LossValue.weight),
                      type_sub.c.name.label(aggregation_type))\
            .select_from(LossValue)\
            .join(riskvalue_aggregationtag) \
            .join(type_sub) \
            .where(filter_condition) \
            .group_by(type_sub.c.name)

        return await pandas_read_sql(stmt, session)
