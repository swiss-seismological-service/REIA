import pandas as pd
from core.parsers import ASSETS_COLS_MAPPING
from core.utils import aggregationtags_from_assets, sites_from_assets
from esloss.datamodel.asset import Asset, AssetCollection, CostType, Site
from esloss.datamodel.calculations import (DamageCalculation, EStatus,
                                           LossCalculation, RiskCalculation)
from esloss.datamodel.vulnerability import (
    BusinessInterruptionVulnerabilityModel, ContentsVulnerabilityModel,
    LossRatio, NonstructuralVulnerabilityModel, OccupantsVulnerabilityModel,
    StructuralVulnerabilityModel, VulnerabilityFunction, VulnerabilityModel)
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

LOSSCATEGORY_OBJECT_MAPPING = {
    'structural': StructuralVulnerabilityModel,
    'nonstructural': NonstructuralVulnerabilityModel,
    'contents': ContentsVulnerabilityModel,
    'businesss_interruption': BusinessInterruptionVulnerabilityModel,
    'occupants': OccupantsVulnerabilityModel}

CALCULATION_MAPPING = {'scenario_risk': RiskCalculation,
                       'scenario_damage': DamageCalculation}


def create_assets(assets: pd.DataFrame,
                  asset_collection_oid: int,
                  session: Session) -> list[Asset]:
    """
    Extract Sites and AggregationTags from Assets, saves them in DB
    as children of the AssetCollection.
    """
    # get AggregationTag types
    aggregation_tags = [
        x for x in assets.columns if x not in list(
            ASSETS_COLS_MAPPING.values()) + ['longitude', 'latitude']]

    # assign AssetCollection to assets
    assets['_assetcollection_oid'] = asset_collection_oid
    assets['aggregationtags'] = assets.apply(lambda _: [], axis=1)

    # create Sites objects and assign them to assets
    sites, assets['site'] = sites_from_assets(
        assets)
    for s in sites:
        s._assetcollection_oid = asset_collection_oid
    assets['site'] = assets.apply(
        lambda x: sites[x['site']], axis=1)

    # create AggregationTag objects and assign them to assets
    for tag in aggregation_tags:
        tags_of_type, assets['aggregationtags_list_index'] = \
            aggregationtags_from_assets(assets, tag)
        for t in tags_of_type:
            t._assetcollection_oid = asset_collection_oid
        assets.apply(lambda x: x['aggregationtags'].append(
            tags_of_type[x['aggregationtags_list_index']]), axis=1)

    # create Asset objects from DataFrame
    valid_cols = list(ASSETS_COLS_MAPPING.values()) + \
        ['site', 'aggregationtags', '_assetcollection_oid']
    asset_objects = map(lambda x: Asset(**x),
                        assets.filter(valid_cols).to_dict('records'))

    session.add_all(list(asset_objects))
    session.commit()

    statement = select(Asset).where(
        Asset._assetcollection_oid == asset_collection_oid)

    return session.execute(statement).unique().scalars().all()


def create_asset_collection(exposure: dict,
                            session: Session) -> AssetCollection:
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


def create_vulnerability_model(
    model: dict,
    session: Session) \
    -> StructuralVulnerabilityModel | \
        OccupantsVulnerabilityModel | NonstructuralVulnerabilityModel | \
        BusinessInterruptionVulnerabilityModel | ContentsVulnerabilityModel:
    """
    Creates a vulnerabilitymodel of the right subtype from a dict
    containing all the data.
    """
    vulnerability_functions = model.pop('vulnerabilityfunctions')

    loss_category = model.pop('losscategory')

    vulnerability_model = LOSSCATEGORY_OBJECT_MAPPING[loss_category](
        **{**model, **{'vulnerabilityfunctions': []}})

    for func in vulnerability_functions:
        loss = func.pop('lossratios')
        function_obj = VulnerabilityFunction(**func)
        function_obj.lossratios = list(map(lambda x: LossRatio(**x),
                                           loss))
        vulnerability_model.vulnerabilityfunctions.append(function_obj)

    session.add(vulnerability_model)
    session.commit()

    return vulnerability_model


def read_sites(asset_collection_oid: int, session: Session) -> list[Site]:
    stmt = select(Site).where(
        Site._assetcollection_oid == asset_collection_oid)
    return session.execute(stmt).scalars().all()


def read_asset_collections(session: Session) -> list[AssetCollection]:
    stmt = select(AssetCollection)
    return session.execute(stmt).unique().scalars().all()


def read_asset_collection(oid, session: Session) -> AssetCollection:
    stmt = select(AssetCollection).where(AssetCollection._oid == oid)
    return session.execute(stmt).unique().scalar()


def delete_asset_collection(
        asset_collection_oid: int,
        session: Session) -> int:
    stmt = delete(AssetCollection).where(
        AssetCollection._oid == asset_collection_oid)
    dlt = session.execute(stmt).rowcount
    session.commit()
    return dlt


def read_vulnerability_models(session: Session) -> list[VulnerabilityModel]:
    stmt = select(VulnerabilityModel)
    return session.execute(stmt).unique().scalars().all()


def read_vulnerability_model(oid: int, session: Session) -> VulnerabilityModel:
    stmt = select(VulnerabilityModel).where(VulnerabilityModel._oid == oid)
    return session.execute(stmt).unique().scalar()


def delete_vulnerability_model(
        vulnerability_model_oid: int,
        session: Session) -> int:
    stmt = delete(VulnerabilityModel).where(
        VulnerabilityModel._oid == vulnerability_model_oid)
    dlt = session.execute(stmt).rowcount
    session.commit()
    return dlt


def create_calculation(
        job: dict,
        session: Session) -> RiskCalculation | DamageCalculation:

    calculation = CALCULATION_MAPPING[job.pop('calculation_mode')]
    calculation = calculation(**job)
    session.add(calculation)
    session.commit()
    return calculation


def read_calculation(oid: int, session: Session) -> LossCalculation:
    stmt = select(LossCalculation).where(LossCalculation._oid == oid)
    return session.execute(stmt).unique().scalar()


def update_calculation_status(calculation_oid: int,
                              status: EStatus,
                              session: Session) -> LossCalculation:
    calculation = read_calculation(calculation_oid, session)
    calculation.status = status
    session.commit()
    return calculation


def read_calculations(session: Session) -> list[LossCalculation]:
    stmt = select(LossCalculation)
    return session.execute(stmt).unique().scalars().all()
