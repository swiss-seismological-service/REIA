import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from reia.webservice.schemas import WSRiskCategory


class AggregationRepositoryOptimized:
    """
    Optimized repository for aggregation-related queries
    with database-side statistics
    """

    @classmethod
    async def get_damage_statistics_optimized(
        cls,
        session: AsyncSession,
        calculation_id: int,
        aggregation_type: str,
        loss_category: WSRiskCategory,
        filter_like_tag: str | None = None
    ) -> pd.DataFrame:
        """
        Get optimized damage statistics with weighted mean and percentiles
        calculated in database
        """

        loss_category_str = loss_category.name  # Use the enum name (uppercase)
        name_pattern = f'%{filter_like_tag}%' if filter_like_tag else '%'

        # Complete optimized SQL query that returns exact webservice format
        sql_query = text("""
        WITH damage_data AS (
            SELECT
                lat.name as tag_name,
                rv.dg1_value,
                rv.dg2_value,
                rv.dg3_value,
                rv.dg4_value,
                rv.dg5_value,
                rv.weight
            FROM loss_riskvalue rv
            INNER JOIN loss_assoc_riskvalue_aggregationtag assoc ON
                rv._oid = assoc.riskvalue
                AND rv._calculation_oid = assoc._calculation_oid
                AND rv.losscategory = assoc.losscategory
            INNER JOIN loss_aggregationtag lat ON
                         assoc.aggregationtag = lat._oid
            WHERE
                rv._calculation_oid = :calculation_id
                AND rv.losscategory = :loss_category_str
                AND rv._type = 'DAMAGE'
                AND assoc.aggregationtype = :aggregation_type
                AND lat.name LIKE :name_pattern
        ),
        damage_statistics AS (
            SELECT
                tag_name,
                -- All damage grade statistics using sparse data functions
                weighted_mean_sparse(array_agg(dg1_value),
                    array_agg(weight)) as dg1_mean,
                weighted_quantile_sparse(array_agg(dg1_value),
                    array_agg(weight), ARRAY[0.1, 0.9]) as dg1_quantiles,

                weighted_mean_sparse(array_agg(dg2_value),
                    array_agg(weight)) as dg2_mean,
                weighted_quantile_sparse(array_agg(dg2_value),
                    array_agg(weight), ARRAY[0.1, 0.9]) as dg2_quantiles,

                weighted_mean_sparse(array_agg(dg3_value),
                    array_agg(weight)) as dg3_mean,
                weighted_quantile_sparse(array_agg(dg3_value),
                    array_agg(weight), ARRAY[0.1, 0.9]) as dg3_quantiles,

                weighted_mean_sparse(array_agg(dg4_value),
                    array_agg(weight)) as dg4_mean,
                weighted_quantile_sparse(array_agg(dg4_value),
                    array_agg(weight), ARRAY[0.1, 0.9]) as dg4_quantiles,

                weighted_mean_sparse(array_agg(dg5_value),
                    array_agg(weight)) as dg5_mean,
                weighted_quantile_sparse(array_agg(dg5_value),
                    array_agg(weight), ARRAY[0.1, 0.9]) as dg5_quantiles
            FROM damage_data
            GROUP BY tag_name
        ),
        all_tags AS (
            SELECT DISTINCT lat.name as tag_name
            FROM loss_aggregationtag lat
            INNER JOIN loss_aggregationgeometry geom ON
                geom._aggregationtag_oid = lat._oid
            WHERE
                lat.type = :aggregation_type
                AND lat.name LIKE :name_pattern
                AND geom._exposuremodel_oid IN (
                    SELECT _exposuremodel_oid
                    FROM loss_calculationbranch
                    WHERE _calculation_oid = :calculation_id
                )
        ),
        building_counts AS (
            SELECT
                lat.name as tag_name,
                SUM(ast.buildingcount) as total_buildings
            FROM loss_aggregationtag lat
            INNER JOIN loss_assoc_asset_aggregationtag assoc ON
                lat._oid = assoc.aggregationtag
            INNER JOIN loss_asset ast ON assoc.asset = ast._oid
            INNER JOIN (
                SELECT _exposuremodel_oid
                FROM loss_calculationbranch
                WHERE _calculation_oid = :calculation_id
                LIMIT 1
            ) exp_sub ON ast._exposuremodel_oid = exp_sub._exposuremodel_oid
            WHERE
                lat.type = :aggregation_type
                AND lat.name LIKE :name_pattern
            GROUP BY lat.name
        )
        SELECT
            :loss_category_value as category,
            ARRAY[at.tag_name] as tag,
            COALESCE(ROUND(ds.dg1_mean::numeric, 5), 0) as dg1_mean,
            COALESCE(ROUND(ds.dg1_quantiles[1]::numeric, 5), 0) as dg1_pc10,
            COALESCE(ROUND(ds.dg1_quantiles[2]::numeric, 5), 0) as dg1_pc90,
            COALESCE(ROUND(ds.dg2_mean::numeric, 5), 0) as dg2_mean,
            COALESCE(ROUND(ds.dg2_quantiles[1]::numeric, 5), 0) as dg2_pc10,
            COALESCE(ROUND(ds.dg2_quantiles[2]::numeric, 5), 0) as dg2_pc90,
            COALESCE(ROUND(ds.dg3_mean::numeric, 5), 0) as dg3_mean,
            COALESCE(ROUND(ds.dg3_quantiles[1]::numeric, 5), 0) as dg3_pc10,
            COALESCE(ROUND(ds.dg3_quantiles[2]::numeric, 5), 0) as dg3_pc90,
            COALESCE(ROUND(ds.dg4_mean::numeric, 5), 0) as dg4_mean,
            COALESCE(ROUND(ds.dg4_quantiles[1]::numeric, 5), 0) as dg4_pc10,
            COALESCE(ROUND(ds.dg4_quantiles[2]::numeric, 5), 0) as dg4_pc90,
            COALESCE(ROUND(ds.dg5_mean::numeric, 5), 0) as dg5_mean,
            COALESCE(ROUND(ds.dg5_quantiles[1]::numeric, 5), 0) as dg5_pc10,
            COALESCE(ROUND(ds.dg5_quantiles[2]::numeric, 5), 0) as dg5_pc90,
            COALESCE(bc.total_buildings::numeric, 0) as buildings
        FROM all_tags at
        LEFT JOIN damage_statistics ds ON at.tag_name = ds.tag_name
        LEFT JOIN building_counts bc ON at.tag_name = bc.tag_name
        ORDER BY at.tag_name
        """)

        result = await session.execute(sql_query, {
            'calculation_id': calculation_id,
            'loss_category_str': loss_category_str,
            'loss_category_value': loss_category.value,
            'aggregation_type': aggregation_type,
            'name_pattern': name_pattern
        })
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns)

    @classmethod
    async def get_loss_statistics_optimized(
        cls,
        session: AsyncSession,
        calculation_id: int,
        aggregation_type: str,
        loss_category: WSRiskCategory,
        filter_like_tag: str | None = None
    ) -> pd.DataFrame:
        """
        Get optimized loss statistics with weighted mean and
        percentiles calculated in database
        """

        loss_category_str = loss_category.name  # Use the enum name (uppercase)
        name_pattern = f'%{filter_like_tag}%' if filter_like_tag else '%'

        # Complete optimized SQL query that returns exact webservice format
        sql_query = text("""
        WITH loss_data AS (
            SELECT
                lat.name as tag_name,
                rv.loss_value,
                rv.weight
            FROM loss_riskvalue rv
            INNER JOIN loss_assoc_riskvalue_aggregationtag assoc ON
                rv._oid = assoc.riskvalue
                AND rv._calculation_oid = assoc._calculation_oid
                AND rv.losscategory = assoc.losscategory
            INNER JOIN loss_aggregationtag lat ON
                         assoc.aggregationtag = lat._oid
            WHERE
                rv._calculation_oid = :calculation_id
                AND rv.losscategory = :loss_category_str
                AND rv._type = 'LOSS'
                AND assoc.aggregationtype = :aggregation_type
                AND lat.name LIKE :name_pattern
        ),
        loss_statistics AS (
            SELECT
                tag_name,
                -- Loss statistics using sparse data functions
                weighted_mean_sparse(array_agg(loss_value),
                    array_agg(weight)) as loss_mean,
                weighted_quantile_sparse(array_agg(loss_value),
                    array_agg(weight), ARRAY[0.1, 0.9]) as loss_quantiles
            FROM loss_data
            GROUP BY tag_name
        ),
        all_tags AS (
            SELECT DISTINCT lat.name as tag_name
            FROM loss_aggregationtag lat
            INNER JOIN loss_aggregationgeometry geom ON
                         geom._aggregationtag_oid = lat._oid
            WHERE
                lat.type = :aggregation_type
                AND lat.name LIKE :name_pattern
                AND geom._exposuremodel_oid IN (
                    SELECT _exposuremodel_oid
                    FROM loss_calculationbranch
                    WHERE _calculation_oid = :calculation_id
                )
        )
        SELECT
            :loss_category_value as category,
            ARRAY[at.tag_name] as tag,
            COALESCE(ROUND(ls.loss_mean::numeric, 5), 0) as loss_mean,
            COALESCE(ROUND(ls.loss_quantiles[1]::numeric, 5), 0) as loss_pc10,
            COALESCE(ROUND(ls.loss_quantiles[2]::numeric, 5), 0) as loss_pc90
        FROM all_tags at
        LEFT JOIN loss_statistics ls ON at.tag_name = ls.tag_name
        ORDER BY at.tag_name
        """)

        result = await session.execute(sql_query, {
            'calculation_id': calculation_id,
            'loss_category_str': loss_category_str,
            'loss_category_value': loss_category.value,
            'aggregation_type': aggregation_type,
            'name_pattern': name_pattern
        })
        rows = result.fetchall()
        columns = result.keys()
        return pd.DataFrame(rows, columns=columns)
