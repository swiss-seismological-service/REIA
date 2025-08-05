CREATE MATERIALIZED VIEW IF NOT EXISTS loss_buildings_per_municipality
AS
SELECT sum(loss_asset.buildingcount) AS total_buildings,
   tags_of_type.name,
   exposuremodel._oid
FROM loss_asset
   JOIN loss_assoc_asset_aggregationtag ON loss_asset._oid = loss_assoc_asset_aggregationtag.asset
   JOIN loss_aggregationtag tags_of_type ON tags_of_type._oid = loss_assoc_asset_aggregationtag.aggregationtag
   JOIN loss_exposuremodel exposuremodel ON exposuremodel._oid = loss_asset._exposuremodel_oid AND tags_of_type.type::text = 'CantonGemeinde'::text
GROUP BY tags_of_type.name, exposuremodel._oid
ORDER BY tags_of_type.name;

CREATE UNIQUE INDEX ON loss_buildings_per_municipality (_oid, name);
CREATE INDEX idx_loss_buildings_per_municipality_name ON loss_buildings_per_municipality (name);
CREATE INDEX idx_loss_buildings_per_municipality_oid ON loss_buildings_per_municipality (_oid);