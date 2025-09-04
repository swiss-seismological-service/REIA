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


ALTER TABLE loss_riskvalue SET (
      -- Vacuum when 5% of table changes (default: 20%)
      autovacuum_vacuum_scale_factor = 0.05,
      -- Analyze when 2.5% changes (default: 10%)
      autovacuum_analyze_scale_factor = 0.025,
      -- Vacuum when just 1000 rows change (for smaller changes)
      autovacuum_vacuum_threshold = 1000,
      -- More aggressive cleanup
      autovacuum_vacuum_cost_delay = 10,  -- Faster vacuum
      autovacuum_vacuum_cost_limit = 2000 -- Higher I/O budget

  );

ALTER TABLE loss_assoc_riskvalue_aggregationtag SET (
      autovacuum_vacuum_scale_factor = 0.05,
      autovacuum_analyze_scale_factor = 0.025,
      autovacuum_vacuum_threshold = 1000,
      autovacuum_vacuum_cost_delay = 10,
      autovacuum_vacuum_cost_limit = 2000

  );

ALTER TABLE loss_aggregationtag SET (
      autovacuum_vacuum_scale_factor = 0.05,
      autovacuum_analyze_scale_factor = 0.025,
      autovacuum_vacuum_threshold = 1000,
      autovacuum_vacuum_cost_delay = 10,
      autovacuum_vacuum_cost_limit = 2000

  );