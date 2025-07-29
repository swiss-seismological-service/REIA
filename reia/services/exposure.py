import pandas as pd
from sqlalchemy.orm import Session

from reia.io import ASSETS_COLS_MAPPING
from reia.repositories.asset import AssetRepository, ExposureModelRepository
from reia.schemas.exposure_schema import ExposureModel


def create_exposure_with_assets(session: Session,
                                exposure: ExposureModel,
                                assets: pd.DataFrame) \
        -> tuple[ExposureModel, list[int], list[int]]:
    """Create an exposure model and its associated assets.

    Args:
        session: Database session.
        exposure: ExposureModel schema object.
        assets: DataFrame containing asset data.

    Returns:
        Created ExposureModel with oid and lists of asset and site OIDs.
    """
    exposuremodel = ExposureModelRepository.create(session, exposure)
    assets_oids, sites_oids = create_assets(assets, exposuremodel.oid, session)
    return exposuremodel, assets_oids, sites_oids


def create_assets(assets: pd.DataFrame,
                  exposure_model_oid: int,
                  session: Session) \
        -> tuple[list[int], list[int]]:
    """Create assets for an exposure model.

    Args:
        assets: DataFrame containing asset data.
        exposure_model_oid: OID of the exposure model.
        session: Database session.

    Returns:
        List of created asset OIDs and site OIDs.
    """
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
    sites, assets['_site_oid'] = _extract_sites(assets)
    sites['_exposuremodel_oid'] = exposure_model_oid

    # create AggregationTags
    assets, aggregationtags, assoc_table = _normalize_tags(
        assets, asset_cols, aggregation_types)
    aggregationtags['_exposuremodel_oid'] = exposure_model_oid

    assets_oids, sites_oids = AssetRepository.insert_from_exposuremodel(
        session,
        sites,
        assets,
        aggregationtags,
        assoc_table)

    return assets_oids, sites_oids


def _extract_sites(assets: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
    """Extract sites from assets dataframe.

    Args:
        assets: Dataframe of assets with 'longitude' and 'latitude' column.

    Returns:
        DataFrame of `n` unique Sites and list of `len(assets)` indices
        mapping each asset to its corresponding site.
    """
    site_keys = list(zip(assets['longitude'], assets['latitude']))
    group_indices, unique_keys = pd.factorize(site_keys)
    unique_sites = pd.DataFrame(unique_keys.tolist(),
                                columns=['longitude', 'latitude'])
    return unique_sites, group_indices.tolist()


def _normalize_tags(df: pd.DataFrame,
                    asset_cols: list[str],
                    tag_cols: list[str]) -> tuple[pd.DataFrame,
                                                  pd.DataFrame,
                                                  pd.DataFrame]:
    """Split a DataFrame into asset values and normalized tags."""
    asset_df = df[asset_cols].copy()

    # Melt tag columns into long format
    tag_df = (
        df[tag_cols]
        .reset_index(drop=True)
        .melt(ignore_index=False, var_name='type', value_name='name')
        .reset_index().rename(columns={'index': 'asset'})
    )

    tag_table, mapping_df = _normalize_tag_pairs(tag_df)
    return asset_df, tag_table, mapping_df


def _normalize_tag_pairs(
        tag_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize (type, name) pairs using pd.factorize."""
    tag_idx, unique_tags = pd.factorize(
        list(zip(tag_df['type'], tag_df['name'])))
    tag_table = pd.DataFrame(unique_tags.tolist(), columns=['type', 'name'])

    mapping_df = tag_df[['asset', 'type']].copy()
    mapping_df['aggregationtag'] = tag_idx
    mapping_df.rename(columns={'type': 'aggregationtype'}, inplace=True)

    return tag_table, mapping_df
