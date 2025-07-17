#    EXAMPLE UPSERT
#     stmt = insert(dm.EarthquakeInformation).values(**earthquake)
#     upsert_stmt = stmt.on_conflict_do_update(
#         constraint='originid_unique', set_=earthquake)
#     earthquake = session.scalars(
#         upsert_stmt.returning(
#             dm.EarthquakeInformation._oid),
#         execution_options={
#             "populate_existing": True}).first()
#     session.commit()


import pandas as pd
from sqlalchemy.orm import Session

from reia.io import ASSETS_COLS_MAPPING
from reia.repositories.asset import AssetRepository
from reia.utils import normalize_assets_tags, sites_from_assets


def create_assets(assets: pd.DataFrame,
                  exposure_model_oid: int,
                  session: Session) -> int:
    # get AggregationTag types
    aggregation_types = [
        x for x in assets.columns if x not in list(
            ASSETS_COLS_MAPPING.values()) + ['longitude', 'latitude']]

    # Asset columns
    asset_cols = list(ASSETS_COLS_MAPPING.values()) + \
        ['_site_oid', '_exposuremodel_oid']

    # assign ExposureModel oid to assets
    assets['_exposuremodel_oid'] = exposure_model_oid

    # create Sites
    sites, assets['_site_oid'] = sites_from_assets(assets)
    sites['_exposuremodel_oid'] = exposure_model_oid

    # create AggregationTags
    assets, aggregationtags, assoc_table = normalize_assets_tags(
        assets, asset_cols, aggregation_types)
    aggregationtags['_exposuremodel_oid'] = exposure_model_oid

    assets_oids = AssetRepository.insert_from_exposuremodel(
        session,
        sites,
        assets,
        aggregationtags,
        assoc_table)

    return assets_oids
