CREATE OR REPLACE FUNCTION calculation_partition_function()
RETURNS TRIGGER AS $$
DECLARE
	partition_name_assoc TEXT;
	partition_name_risk TEXT;
BEGIN
 	partition_name_assoc := 'loss_assoc_' || NEW._oid;
	partition_name_risk := 'loss_riskvalue_' || NEW._oid;
IF NOT EXISTS
	(SELECT 1
   	 FROM   information_schema.tables 
   	 WHERE  table_name = partition_name_assoc) 
THEN
	RAISE NOTICE 'A partition has been created %', partition_name_assoc;
	EXECUTE format(E'CREATE TABLE %I PARTITION OF loss_assoc_riskvalue_aggregationtag FOR VALUES IN (%s) PARTITION BY LIST(losscategory)', partition_name_assoc, NEW._oid);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''CONTENTS'')', format(E'%s_contents',partition_name_assoc), partition_name_assoc);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''BUSINESS_INTERRUPTION'')', format(E'%s_business_interruption',partition_name_assoc), partition_name_assoc);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''NONSTRUCTURAL'')', format(E'%s_nonstructural',partition_name_assoc), partition_name_assoc);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''OCCUPANTS'')', format(E'%s_occupants',partition_name_assoc), partition_name_assoc);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''STRUCTURAL'')', format(E'%s_structural',partition_name_assoc), partition_name_assoc);
END IF;
IF NOT EXISTS
	(SELECT 1
   	 FROM   information_schema.tables 
   	 WHERE  table_name = partition_name_risk) 
THEN
	RAISE NOTICE 'A partition has been created %', partition_name_risk;
	EXECUTE format(E'CREATE TABLE %I PARTITION OF loss_riskvalue FOR VALUES IN (%s) PARTITION BY LIST(losscategory)', partition_name_risk, NEW._oid);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''CONTENTS'')', format(E'%s_contents',partition_name_risk), partition_name_risk);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''BUSINESS_INTERRUPTION'')', format(E'%s_business_interruption',partition_name_risk), partition_name_risk);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''NONSTRUCTURAL'')', format(E'%s_nonstructural',partition_name_risk), partition_name_risk);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''OCCUPANTS'')', format(E'%s_occupants',partition_name_risk), partition_name_risk);
	EXECUTE format(E'CREATE TABLE %I PARTITION OF %I FOR VALUES IN (''STRUCTURAL'')', format(E'%s_structural',partition_name_risk), partition_name_risk);
END IF;
RETURN NEW;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER insert_calculation_trigger
    BEFORE INSERT ON public.loss_calculation
    FOR EACH ROW EXECUTE PROCEDURE public.calculation_partition_function();