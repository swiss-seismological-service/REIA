import pandas as pd
from esloss.datamodel import EarthquakeInformation
from esloss.datamodel.asset import (AggregationTag, Asset, AssetCollection,
                                    CostType, Site)
from esloss.datamodel.calculations import (Calculation, DamageCalculation,
                                           DamageCalculationBranch, EStatus,
                                           RiskCalculation,
                                           RiskCalculationBranch)
from esloss.datamodel.lossvalues import AggregatedLoss
from esloss.datamodel.vulnerability import (
    BusinessInterruptionVulnerabilityModel, ContentsVulnerabilityModel,
    LossRatio, NonstructuralVulnerabilityModel, OccupantsVulnerabilityModel,
    StructuralVulnerabilityModel, VulnerabilityFunction, VulnerabilityModel)
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.io.parse_input import ASSETS_COLS_MAPPING
from core.utils import aggregationtags_from_assets, sites_from_assets

LOSSCATEGORY_OBJECT_MAPPING = {
    'structural': StructuralVulnerabilityModel,
    'nonstructural': NonstructuralVulnerabilityModel,
    'contents': ContentsVulnerabilityModel,
    'business_interruption': BusinessInterruptionVulnerabilityModel,
    'occupants': OccupantsVulnerabilityModel}

CALCULATION_MAPPING = {'scenario_risk': RiskCalculation,
                       'scenario_damage': DamageCalculation}

CALCULATION_BRANCH_MAPPING = {'scenario_risk': RiskCalculationBranch,
                              'scenario_damage': DamageCalculationBranch}


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
    stmt = select(AssetCollection).order_by(AssetCollection._oid)
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
    stmt = select(VulnerabilityModel).order_by(VulnerabilityModel._oid)
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


def create_or_update_earthquake_information(
        earthquake: dict,
        session: Session) -> EarthquakeInformation:

    stmt = insert(EarthquakeInformation).values(**earthquake)
    upsert_stmt = stmt.on_conflict_do_update(
        constraint='eventid_unique', set_=earthquake)
    earthquake = session.scalars(
        upsert_stmt.returning(
            EarthquakeInformation._oid),
        execution_options={
            "populate_existing": True}).first()
    # session.commit()
    return earthquake


def create_calculation(
        job: dict,
        session: Session) -> RiskCalculation | DamageCalculation:

    calculation = CALCULATION_MAPPING[job.pop('calculation_mode')]
    calculation = calculation(**job)
    session.add(calculation)
    session.commit()
    return calculation


def create_calculation_branch(branch: dict,
                              session: Session,
                              calculation_oid: int = None) \
        -> RiskCalculationBranch | DamageCalculationBranch:

    if calculation_oid:
        branch['_calculation_oid'] = calculation_oid

    calculation_branch = CALCULATION_BRANCH_MAPPING[branch.pop(
        'calculation_mode')]

    calculation_branch = calculation_branch(**branch)
    session.add(calculation_branch)
    session.commit()

    return calculation_branch


def read_calculation(oid: int, session: Session) -> Calculation:
    stmt = select(Calculation).where(Calculation._oid == oid)
    return session.execute(stmt).unique().scalar()


def update_calculation_status(calculation_oid: int,
                              status: EStatus,
                              session: Session) -> Calculation:
    calculation = read_calculation(calculation_oid, session)
    calculation.status = status
    session.commit()
    return calculation


def read_calculations(session: Session) -> list[Calculation]:
    stmt = select(Calculation).order_by(Calculation._oid)
    return session.execute(stmt).unique().scalars().all()


def create_aggregated_losses(
        losses: pd.DataFrame,
        aggregationtypes: list[str],
        calculation_oid: int,
        assetcollection_oid: int,
        session: Session) -> list[AggregatedLoss]:

    aggregations = {}
    for type in aggregationtypes:
        stmt = select(AggregationTag).where(
            AggregationTag.type == type,
            AggregationTag._assetcollection_oid == assetcollection_oid)
        type_tags = session.execute(stmt).scalars().all()
        aggregations.update({tag.name: tag for tag in type_tags})

    losses['aggregationtags'] = losses['aggregationtags'].apply(
        lambda x: [aggregations[y] for y in x])

    loss_objects = list(map(lambda x: AggregatedLoss(
        **x, _riskcalculation_oid=calculation_oid),
        losses.to_dict('records')))

    session.add_all(loss_objects)
    session.commit()

    return loss_objects
