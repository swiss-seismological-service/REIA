import pandas as pd

from esloss.datamodel.asset import (
    AssetCollection, Asset, CostType)
from esloss.datamodel.vulnerability import (
    VulnerabilityFunction, VulnerabilityModel)

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.utils import aggregationtags_from_assets, sites_from_assets
from core.db import session
from core.parsers import ASSETS_COLS_MAPPING


def create_assets(assets: pd.DataFrame,
                  asset_collection: AssetCollection,
                  session: Session):

    # get AggregationTag types
    aggregation_tags = [
        x for x in assets.columns if x not in list(
            ASSETS_COLS_MAPPING.values()) + ['longitude', 'latitude']]

    # assign AssetCollection to assets
    assets['_assetcollection_oid'] = asset_collection._oid
    assets['aggregationtags'] = assets.apply(lambda _: [], axis=1)

    # create Sites objects and assign them to assets
    sites, assets['site'] = sites_from_assets(
        assets)
    for s in sites:
        s._assetcollection_oid = asset_collection._oid
    assets['site'] = assets.apply(
        lambda x: sites[x['site']], axis=1)

    # create AggregationTag objects and assign them to assets
    for tag in aggregation_tags:
        tags_of_type, assets['aggregationtags_list_index'] = \
            aggregationtags_from_assets(assets, tag)
        for t in tags_of_type:
            t._assetcollection_oid = asset_collection._oid
        assets.apply(lambda x: x['aggregationtags'].append(
            tags_of_type[x['aggregationtags_list_index']]), axis=1)

    # create Asset objects from
    valid_cols = list(ASSETS_COLS_MAPPING.values()) + \
        ['site', 'aggregationtags', '_assetcollection_oid']
    asset_objects = map(lambda x: Asset(**x),
                        assets.filter(valid_cols).to_dict('records'))

    session.add_all(list(asset_objects))
    session.commit()

    statement = select(Asset).where(
        Asset._assetcollection_oid == asset_collection._oid)

    return session.execute(statement).scalars().all()


def create_asset_collection(exposure: dict, session: Session) -> int:
    """
    Creates an AssetCollection and the respective CostTypes from a dict and
    saves it to the Database.
    """

    cost_types = exposure.pop('costtypes')
    asset_collection = AssetCollection(**exposure)

    for ct in cost_types:
        asset_collection.costtypes.append(CostType(**ct))

    session.add(asset_collection)
    session.commit()

    return asset_collection


def create_vulnerability_model(model: dict, functions: list) -> int:
    # assemble vulnerability Model
    vulnerability_model = VulnerabilityModel(**model)
    session.add(vulnerability_model)
    session.flush()

    # assemble vulnerability Functions
    for vF in functions:
        f = VulnerabilityFunction(**vF)
        f._vulnerabilitymodel_oid = vulnerability_model._oid
        session.add(f)

    session.commit()
    return vulnerability_model._oid
