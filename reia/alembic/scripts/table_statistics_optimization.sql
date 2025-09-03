-- Higher statistics targets for partitioned table query optimization
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
ALTER TABLE loss_assoc_riskvalue_aggregationtag ALTER COLUMN 
       riskvalue SET STATISTICS 1000;
CREATE STATISTICS stat_assoc_correlation ON
      riskvalue, _calculation_oid, losscategory, aggregationtype
  FROM loss_assoc_riskvalue_aggregationtag;