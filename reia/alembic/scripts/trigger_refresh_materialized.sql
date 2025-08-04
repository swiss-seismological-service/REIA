CREATE OR REPLACE FUNCTION refresh_materialized_loss_buildings()
RETURNS TRIGGER AS $$
BEGIN

    REFRESH MATERIALIZED VIEW loss_buildings_per_municipality;

    RETURN NULL;

END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER refresh_materialized_loss_buildings_trigger
    AFTER INSERT OR UPDATE OR DELETE ON loss_asset
    FOR EACH STATEMENT EXECUTE PROCEDURE refresh_materialized_loss_buildings();