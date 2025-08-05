CREATE OR REPLACE FUNCTION aggregationtag_partition_function()
RETURNS TRIGGER AS $$
DECLARE
	partition_name_type TEXT;
	_elem TEXT;
BEGIN
	FOREACH _elem IN ARRAY NEW.aggregationtypes
	LOOP
	 	partition_name_type := 'loss_aggregationtag_' || _elem;
		IF NOT EXISTS
			(SELECT 1
			 FROM   information_schema.tables 
			 WHERE  table_name = partition_name_type) 
		THEN
			RAISE NOTICE 'A partition has been created %', partition_name_type;
			EXECUTE format(E'CREATE TABLE %I PARTITION OF loss_aggregationtag FOR VALUES IN (''%s'')', partition_name_type, _elem);
		END IF;
	END LOOP;
RETURN NEW;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER insert_aggregationtag_trigger
    BEFORE INSERT ON loss_exposuremodel
    FOR EACH ROW EXECUTE PROCEDURE aggregationtag_partition_function();
