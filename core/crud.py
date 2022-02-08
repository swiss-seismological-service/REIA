from typing import List
import pandas as pd

from esloss.datamodel import (  # noqa
    AssetCollection, Asset, Site,
    VulnerabilityFunction, VulnerabilityModel,
    LossConfig, LossModel, LossCalculation,
    Municipality, PostalCode)
from sqlalchemy import select

from core.utils import sites_from_assets
from core.parsers import risk_dict_to_lossmodel_dict
from core.database import session, engine


def create_asset_collection(exposure: dict, assets: pd.DataFrame):

    asset_collection = AssetCollection(**exposure)
    # add tags to session and tag names to Asset Collection
    asset_collection.tagnames = []
    if '_municipality_oid' in assets:
        asset_collection.tagnames.append('municipality')
        for el in assets['_municipality_oid'].unique():
            session.merge(Municipality(_oid=int(el)))
    if '_postalcode_oid' in assets:
        asset_collection.tagnames.append('postalcode')
        for el in assets['_postalcode_oid'].unique():
            session.merge(PostalCode(_oid=int(el)))

    # flush assetcollection to get id
    session.add(asset_collection)
    session.flush()

    # assign assetcollection
    assets['_assetcollection_oid'] = asset_collection._oid

    # create sites and assign sites list index to assets
    sites, assets['sites_list_index'] = sites_from_assets(
        assets)

    # add and flush sites to get an ID but keep fast accessible in memory
    session.add_all(sites)
    session.flush()

    # assign ID back to dataframe using group index
    assets['_site_oid'] = assets.apply(
        lambda x: sites[x['sites_list_index']]._oid, axis=1)

    # commit so that FK exists in databse
    session.commit()

    # write selected columns directly to database
    assets.filter(Asset.get_keys()).to_sql(
        'loss_asset', engine, if_exists='append', index=False)

    return asset_collection._oid


def create_vulnerability_model(model: dict, functions: list):
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


def create_loss_model(
        risk: dict,
        asset_collection_oid: int,
        vulnerability_models_oid: List[int]):

    data = risk_dict_to_lossmodel_dict(risk)

    # add asset collection id
    data['_assetcollection_oid'] = asset_collection_oid
    data['preparationcalculationmode'] = 'scenario'

    # get vulnerability models
    statement = select(VulnerabilityModel).filter(
        VulnerabilityModel._oid.in_(vulnerability_models_oid))
    vulnerabilitymodels = session.execute(statement).scalars().unique()
    data['vulnerabilitymodels'] = list(vulnerabilitymodels)

    # assemble LossModel object
    loss_model = LossModel(**data)
    session.add(loss_model)
    session.commit()

    return loss_model._oid
