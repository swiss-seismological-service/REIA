import time
import pandas as pd

from esloss.datamodel.asset import (
    AssetCollection, Asset, CostType)
from esloss.datamodel.vulnerability import (
    VulnerabilityFunction, VulnerabilityModel)

from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from core.utils import aggregationtags_from_assets, sites_from_assets
from core.db import session, engine
from core.parsers import ASSETS_COLS_MAPPING


def create_assets(assets: pd.DataFrame,
                  asset_collection: AssetCollection,
                  session: Session):

    # assign assetcollection
    assets['_assetcollection_oid'] = asset_collection._oid

    # create sites and assign sites list index to assets
    sites, assets['sites_list_index'] = sites_from_assets(
        assets)

    aggregation_tags, assets['aggregationtags_list_index'] = \
        aggregationtags_from_assets(assets, 'Canton')

    # print(aggregation_tags)
    # print(group_list)

    # add and commit sites to get an ID
    # session.add_all(sites)
    # session.commit()

    # assign ID back to dataframe using group index
    assets['site'] = assets.apply(
        lambda x: sites[x['sites_list_index']], axis=1)

    assets['aggregationtags'] = assets.apply(lambda _: [], axis=1)
    assets.apply(lambda x: x['aggregationtags'].append(
        aggregation_tags[x['aggregationtags_list_index']]), axis=1)

    # print(assets.head)
    start = time.perf_counter()
    asset_objects = map(
        lambda x: Asset(**x),
        assets.filter(Asset.get_keys()
                      + ['site', 'aggregationtags']).to_dict('records'))

    session.add_all(asset_objects)
    session.commit()
    print(time.perf_counter() - start)

    # write selected columns directly to database
    # assets['_site_oid'] = assets.apply(
    #     lambda x: sites[x['sites_list_index']]._oid, axis=1)
    # assets.filter(Asset.get_keys()).to_sql(
    #     'loss_asset', engine, if_exists='append', index=False)

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
