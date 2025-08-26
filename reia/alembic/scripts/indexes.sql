-- 1. Composite index for aggregation geometry joins
-- Optimizes the join between aggregationtag and aggregationgeometry tables
-- Converts hash join to index scan
CREATE INDEX IF NOT EXISTS idx_aggregationgeometry_composite 
ON loss_aggregationgeometry (_aggregationtag_oid, _exposuremodel_oid);

-- 2. Index for risk value lookups
-- Enables efficient index scan instead of sequential scan on loss_riskvalue
-- Critical for joining with association table
CREATE INDEX IF NOT EXISTS idx_riskvalue_calc_category_type 
ON loss_riskvalue (_calculation_oid, losscategory, _type, _oid);

-- 3. Covering index for association table
-- Enables index-only scan on loss_assoc_riskvalue_aggregationtag
-- Significantly reduces I/O by avoiding table lookups
CREATE INDEX IF NOT EXISTS idx_assoc_join_optimized 
ON loss_assoc_riskvalue_aggregationtag (aggregationtype, _calculation_oid, losscategory, aggregationtag, riskvalue);

-- 4. Index for calculation branch lookups
-- Optimizes the subquery for finding exposure models
-- Enables index-only scan instead of sequential scan
CREATE INDEX IF NOT EXISTS idx_calculationbranch_calc_exposure 
ON loss_calculationbranch (_calculation_oid, _exposuremodel_oid);

-- 5. Pattern-optimized index for aggregation tag filtering
-- Optimizes type filtering with LIKE pattern matching on name
-- Uses text_pattern_ops for efficient pattern matching queries
CREATE INDEX IF NOT EXISTS idx_aggregationtag_type_name_oid 
ON loss_aggregationtag (type, name text_pattern_ops, _oid);

-- 6. Asset lookup index
-- Optimizes asset queries by exposure model and site
-- Enables efficient joins with aggregation tags
CREATE INDEX IF NOT EXISTS idx_asset_exposure_site 
ON loss_asset (_exposuremodel_oid, _site_oid, _oid);

-- 7. Asset-aggregationtag association index
-- Optimizes the join between assets and aggregation tags
-- Critical for building count calculations
CREATE INDEX IF NOT EXISTS idx_assoc_asset_aggregationtag_lookup 
ON loss_assoc_asset_aggregationtag (aggregationtag, asset, aggregationtype);

-- 8. Materialized view name lookup index
-- Optimizes name-based lookups on materialized view
-- Used for building statistics queries
CREATE INDEX IF NOT EXISTS idx_loss_buildings_per_municipality_name 
ON loss_buildings_per_municipality (name);

-- 9. Covering index for risk value operations (OPTIONAL)
-- Trade-off: Large index size due to included columns
-- CREATE INDEX IF NOT EXISTS idx_riskvalue_covering 
-- ON loss_riskvalue (_oid, _calculation_oid, losscategory, _type, loss_value, weight);