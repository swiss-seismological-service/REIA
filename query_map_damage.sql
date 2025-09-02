-- Optimized damage mapping query using aggregation.py techniques
-- Parameters: calculation_id, losscategory, aggregation_type, name_pattern
WITH exposure_weights AS (
    SELECT 
        em._oid as exposure_oid, 
        SUM(cb.weight) as total_weight
    FROM loss_calculation c
    INNER JOIN loss_calculationbranch cb ON c._oid = cb._calculation_oid
    INNER JOIN loss_exposuremodel em ON em._oid = cb._exposuremodel_oid
    WHERE c._oid = 2  -- calculation_id variable
    GROUP BY em._oid
),
total_buildings AS (
    SELECT 
        SUM(bpm.total_buildings * ew.total_weight) as total_buildings,
        bpm.name
    FROM loss_buildings_per_municipality bpm
    INNER JOIN exposure_weights ew ON ew.exposure_oid = bpm._oid
    GROUP BY bpm.name
),
-- Optimized damage aggregation using sparse-aware weighted functions
damage_by_aggregation AS (
    SELECT
        assoc.aggregationtag,
        lat.name as tag_name,
        weighted_mean_sparse(
            array_agg(rv.dg2_value + rv.dg3_value + rv.dg4_value + rv.dg5_value),
            array_agg(rv.weight)
        ) AS damaged_buildings
    FROM loss_riskvalue rv
    INNER JOIN loss_assoc_riskvalue_aggregationtag assoc ON 
        rv._oid = assoc.riskvalue
        AND rv._calculation_oid = assoc._calculation_oid
        AND rv.losscategory = assoc.losscategory
    INNER JOIN loss_aggregationtag lat ON assoc.aggregationtag = lat._oid
    WHERE 
        rv._calculation_oid = 2  -- calculation_id variable
        AND rv.losscategory = 'BUSINESS_INTERRUPTION'  -- losscategory variable
        AND rv._type = 'DAMAGE'
        AND assoc.aggregationtype = 'CantonGemeinde'  -- aggregation_type variable
        AND lat.name LIKE 'AG%'  -- name_pattern variable
    GROUP BY assoc.aggregationtag, lat.name
),
-- Optimized geometry aggregation using composite indexes
tags_with_geometry AS (
    SELECT
        tag._oid as tag_oid,
        tag.name AS tag_name,
        geom.name AS municipality_name,
        geom.geometry,
        geom._oid AS gid
    FROM loss_aggregationtag tag
    INNER JOIN loss_aggregationgeometry geom ON geom._aggregationtag_oid = tag._oid
    INNER JOIN exposure_weights ew ON geom._exposuremodel_oid = ew.exposure_oid
    WHERE 
        tag.type = 'CantonGemeinde'  -- aggregation_type variable
        AND tag.name LIKE 'AG%'  -- name_pattern variable
)
SELECT
    ROUND(COALESCE(SUM(dba.damaged_buildings), 0) / NULLIF(ANY_VALUE(tb.total_buildings), 0) * 100) AS damage,
    twg.tag_name,
    twg.municipality_name,
    ANY_VALUE(twg.gid) as gid,
    LEFT(twg.tag_name, 2) AS gdektg,
    ST_Collect(twg.geometry) AS the_geom  -- Use ST_Collect instead of ST_UNION for better performance
FROM tags_with_geometry twg
INNER JOIN total_buildings tb ON twg.tag_name = tb.name
LEFT JOIN damage_by_aggregation dba ON twg.tag_oid = dba.aggregationtag
GROUP BY twg.tag_name, twg.municipality_name
ORDER BY twg.tag_name;