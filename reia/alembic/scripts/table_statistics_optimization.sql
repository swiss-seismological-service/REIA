-- Higher statistics targets for partitioned table query optimization
-- These columns are used in complex joins and benefit from detailed statistics

ALTER TABLE loss_riskvalue ALTER COLUMN
       _calculation_oid SET STATISTICS 1000;
ALTER TABLE loss_riskvalue ALTER COLUMN
       losscategory SET STATISTICS 1000;
ALTER TABLE loss_assoc_riskvalue_aggregationtag ALTER COLUMN
       _calculation_oid SET STATISTICS 1000;
ALTER TABLE loss_assoc_riskvalue_aggregationtag ALTER COLUMN
       losscategory SET STATISTICS 1000;
ALTER TABLE loss_assoc_riskvalue_aggregationtag ALTER COLUMN
       aggregationtype SET STATISTICS 1000;