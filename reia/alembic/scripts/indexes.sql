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

-- 5. Composite index for aggregation tag filtering
-- Optimizes type and name pattern matching
-- Enables index-only scan for aggregationtag type lookups
CREATE INDEX IF NOT EXISTS idx_aggregationtag_type_name_oid 
ON loss_aggregationtag (type, name, _oid);

-- 6. Covering index for risk value operations (OPTIONAL)
-- Trade-off: Large index size due to included columns
-- CREATE INDEX IF NOT EXISTS idx_riskvalue_covering 
-- ON loss_riskvalue (_oid, _calculation_oid, losscategory, _type, loss_value, weight);