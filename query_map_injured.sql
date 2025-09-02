-- Optimized injury/casualty mapping query using aggregation.py techniques
-- Parameters: calculation_id, loss_category, aggregation_type, name_pattern
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
        rv._calculation_oid = 1  -- calculation_id parameter
        AND rv.losscategory = 'NONSTRUCTURAL'  -- loss_category parameter
        AND rv._type = 'LOSS'
        AND assoc.aggregationtype = 'CantonGemeinde'  -- aggregation_type parameter
        AND lat.name LIKE 'AG%'  -- name_pattern parameter
),
loss_statistics AS (
    SELECT
        tag_name,
        weighted_mean_sparse(
            array_agg(loss_value),
            array_agg(weight)
        ) as sum_injured
    FROM loss_data
    GROUP BY tag_name
),
all_tags AS (
    SELECT DISTINCT 
        lat.name as tag_name
    FROM loss_aggregationtag lat
    INNER JOIN loss_aggregationgeometry geom ON
        geom._aggregationtag_oid = lat._oid
    WHERE
        lat.type = 'CantonGemeinde'  -- aggregation_type parameter
        AND lat.name LIKE 'AG%'  -- name_pattern parameter
        AND geom._exposuremodel_oid IN (
            SELECT _exposuremodel_oid
            FROM loss_calculationbranch
            WHERE _calculation_oid = 1  -- calculation_id parameter
        )
),
geometry_data AS (
    SELECT 
        lat.name as tag_name,
        MIN(geom._oid) as gid,
        STRING_AGG(DISTINCT geom.name, ', ') as municipality_name,
        ST_Collect(geom.geometry) as the_geom  -- Use ST_Collect instead of ST_UNION for better performance
    FROM loss_aggregationtag lat
    INNER JOIN loss_aggregationgeometry geom ON geom._aggregationtag_oid = lat._oid
    WHERE
        lat.type = 'CantonGemeinde'  -- aggregation_type parameter
        AND lat.name LIKE 'AG%'  -- name_pattern parameter
        AND geom._exposuremodel_oid IN (
            SELECT _exposuremodel_oid
            FROM loss_calculationbranch
            WHERE _calculation_oid = 1  -- calculation_id parameter
        )
    GROUP BY lat.name
)
SELECT
    COALESCE(ls.sum_injured, 0) as injured,
    at.tag_name,
    gd.gid,
    gd.municipality_name,
    gd.the_geom
FROM all_tags at
LEFT JOIN loss_statistics ls ON at.tag_name = ls.tag_name
LEFT JOIN geometry_data gd ON at.tag_name = gd.tag_name
ORDER BY at.tag_name;
