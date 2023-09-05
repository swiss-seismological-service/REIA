import os
from io import StringIO
from multiprocessing import Pool
from operator import attrgetter

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import reia.datamodel as dm
from reia.io import (ASSETS_COLS_MAPPING, CALCULATION_BRANCH_MAPPING,
                     CALCULATION_MAPPING, LOSSCATEGORY_FRAGILITY_MAPPING,
                     LOSSCATEGORY_VULNERABILITY_MAPPING)
from reia.utils import aggregationtags_from_assets, sites_from_assets

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


def create_assets(assets: pd.DataFrame,
                  exposure_model_oid: int,
                  session: Session) -> list[dm.Asset]:
    """
    Extract Sites and AggregationTags from Assets, saves them in DB
    as children of the dm.ExposureModel.
    """
    # get dm.AggregationTag types
    aggregation_types = [
        x for x in assets.columns if x not in list(
            ASSETS_COLS_MAPPING.values()) + ['longitude', 'latitude']]

    # assign dm.ExposureModel to assets
    assets['_exposuremodel_oid'] = exposure_model_oid
    assoc_table = pd.DataFrame(
        {'aggregationtags': assets.apply(lambda _: [], axis=1)})

    # create Sites objects and assign them to assets
    sites, assets['_site_oid'] = sites_from_assets(assets)
    for s in sites:
        s._exposuremodel_oid = exposure_model_oid
    session.add_all(sites)
    session.flush()
    assets['_site_oid'] = assets['_site_oid'].map(lambda x: sites[x]._oid)

    # create dm.AggregationTag objects and assign them to assets
    for tag_type in aggregation_types:
        existing_tags = read_aggregationtags(
            tag_type, exposure_model_oid, session)
        tags_of_type, assoc_table['aggregationtags_list_index'] = \
            aggregationtags_from_assets(assets, tag_type, existing_tags)
        session.add_all(list(tags_of_type))
        session.flush()
        assoc_table.apply(lambda x: x['aggregationtags'].append(
            tags_of_type[x['aggregationtags_list_index']]), axis=1)

    session.commit()

    # create dm.Asset objects from DataFrame
    valid_cols = list(ASSETS_COLS_MAPPING.values()) + \
        ['_site_oid', 'aggregationtags', '_exposuremodel_oid']

    assoc_table['asset'] = list(session.scalars(insert(dm.Asset).returning(
        dm.Asset._oid), assets.filter(valid_cols).to_dict('records')).all())

    assoc_table = assoc_table.explode('aggregationtags', ignore_index=True)
    assoc_table['aggregationtag'] = assoc_table['aggregationtags'].map(
        attrgetter('_oid'))
    assoc_table['aggregationtype'] = assoc_table['aggregationtags'].map(
        attrgetter('type'))
    assoc_table = assoc_table.drop(
        ['aggregationtags', 'aggregationtags_list_index'], axis=1)

    session.commit()

    copy_raw(assoc_table, 'loss_assoc_asset_aggregationtag')

    statement = select(dm.Asset).where(
        dm.Asset._exposuremodel_oid == exposure_model_oid)

    return session.execute(statement).unique().scalars().all()


def create_asset_collection(exposure: dict,
                            session: Session) -> dm.ExposureModel:
    """
    Creates an dm.ExposureModel and the respective CostTypes from a dict and
    saves it to the Database.
    """

    cost_types = exposure.pop('costtypes')
    asset_collection = dm.ExposureModel(**exposure)

    for ct in cost_types:
        asset_collection.costtypes.append(dm.CostType(**ct))

    session.add(asset_collection)
    session.commit()

    return asset_collection


def create_fragility_model(
        model: dict,
        session: Session) \
    -> dm.StructuralFragilityModel | dm.NonstructuralFragilityModel | \
        dm.BusinessInterruptionFragilityModel | dm.ContentsFragilityModel:
    '''
    Creates a fragilitymodel of the right subtype from a dict containing
    all the data.
    '''
    fragility_functions = model.pop('fragilityfunctions')
    loss_category = model.pop('losscategory')
    fragility_model = LOSSCATEGORY_FRAGILITY_MAPPING[loss_category](
        **{**model, **{'fragilityfunctions': []}})

    for func in fragility_functions:
        limit = func.pop('limitstates')
        function_obj = dm.FragilityFunction(**func)
        function_obj.limitstates = list(
            map(lambda x: dm.LimitState(**x), limit))
        fragility_model.fragilityfunctions.append(function_obj)

    session.add(fragility_model)
    session.commit()

    return fragility_model


def create_taxonomy_map(
        mapping: pd.DataFrame,
        name: str,
        session: Session) -> dm.TaxonomyMap:
    '''
    Creates a TaxonomyMapping.
    '''
    taxonomy_map = dm.TaxonomyMap(name=name)
    taxonomy_map.mappings = list(
        map(lambda x: dm.Mapping(**x), mapping.to_dict(orient='records')))

    session.add(taxonomy_map)
    session.commit()

    return taxonomy_map


def read_taxonomymaps(session: Session) -> list[dm.TaxonomyMap]:
    stmt = select(dm.TaxonomyMap).order_by(dm.TaxonomyMap._oid)
    return session.execute(stmt).unique().scalars().all()


def read_taxonomymap(oid: int, session: Session) -> dm.TaxonomyMap:
    stmt = select(dm.TaxonomyMap).where(dm.TaxonomyMap._oid == oid)
    return session.execute(stmt).unique().scalar()


def delete_taxonomymap(
        oid: int,
        session: Session) -> int:
    stmt = delete(dm.TaxonomyMap).where(
        dm.TaxonomyMap._oid == oid)
    session.execute(stmt)
    session.commit()


def create_vulnerability_model(
    model: dict,
    session: Session) \
    -> dm.StructuralVulnerabilityModel | \
        dm.OccupantsVulnerabilityModel | \
        dm.NonstructuralVulnerabilityModel | \
        dm.BusinessInterruptionVulnerabilityModel | \
        dm.ContentsVulnerabilityModel:
    """
    Creates a vulnerabilitymodel of the right subtype from a dict
    containing all the data.
    """
    vulnerability_functions = model.pop('vulnerabilityfunctions')

    loss_category = model.pop('losscategory')

    vulnerability_model = LOSSCATEGORY_VULNERABILITY_MAPPING[loss_category](
        **{**model, **{'vulnerabilityfunctions': []}})

    for func in vulnerability_functions:
        loss = func.pop('lossratios')
        function_obj = dm.VulnerabilityFunction(**func)
        function_obj.lossratios = list(map(lambda x: dm.LossRatio(**x),
                                           loss))
        vulnerability_model.vulnerabilityfunctions.append(function_obj)

    session.add(vulnerability_model)
    session.commit()

    return vulnerability_model


def read_sites(asset_collection_oid: int, session: Session) -> list[dm.Site]:
    stmt = select(dm.Site).where(
        dm.Site._exposuremodel_oid == asset_collection_oid)
    return session.execute(stmt).scalars().all()


def read_asset_collections(session: Session) -> list[dm.ExposureModel]:
    stmt = select(dm.ExposureModel).order_by(dm.ExposureModel._oid)
    return session.execute(stmt).unique().scalars().all()


def read_asset_collection(oid, session: Session) -> dm.ExposureModel:
    stmt = select(dm.ExposureModel).where(dm.ExposureModel._oid == oid)
    return session.execute(stmt).unique().scalar()


def delete_asset_collection(asset_collection_oid: int,
                            session: Session) -> int:
    stmt = delete(dm.ExposureModel).where(
        dm.ExposureModel._oid == asset_collection_oid)
    session.execute(stmt)
    session.commit()


def read_fragility_models(session: Session) -> list[dm.FragilityModel]:
    stmt = select(dm.FragilityModel).order_by(dm.FragilityModel._oid)
    return session.execute(stmt).unique().scalars().all()


def read_fragility_model(oid: int, session: Session) -> dm.FragilityModel:
    stmt = select(dm.FragilityModel).where(dm.FragilityModel._oid == oid)
    return session.execute(stmt).unique().scalar()


def delete_fragility_model(
        fragility_model_oid: int,
        session: Session) -> int:
    stmt = delete(dm.FragilityModel).where(
        dm.FragilityModel._oid == fragility_model_oid)
    session.execute(stmt)
    session.commit()


def read_vulnerability_models(session: Session) -> list[dm.VulnerabilityModel]:
    stmt = select(dm.VulnerabilityModel).order_by(dm.VulnerabilityModel._oid)
    return session.execute(stmt).unique().scalars().all()


def read_vulnerability_model(
        oid: int,
        session: Session) -> dm.VulnerabilityModel:
    stmt = select(
        dm.VulnerabilityModel).where(
        dm.VulnerabilityModel._oid == oid)
    return session.execute(stmt).unique().scalar()


def delete_vulnerability_model(
        vulnerability_model_oid: int,
        session: Session) -> int:
    stmt = delete(dm.VulnerabilityModel).where(
        dm.VulnerabilityModel._oid == vulnerability_model_oid)
    session.execute(stmt)
    session.commit()


def create_calculation(
        job: dict,
        session: Session) -> dm.LossCalculation | dm.DamageCalculation:

    calculation = CALCULATION_MAPPING[job.pop('calculation_mode')]
    calculation = calculation(**job)
    session.add(calculation)
    session.commit()
    return calculation


def read_calculation_branch(oid: int,
                            session: Session) -> dm.CalculationBranch:
    stmt = select(dm.CalculationBranch).where(dm.CalculationBranch._oid == oid)
    return session.execute(stmt).unique().scalar()


def create_calculation_branch(branch: dict,
                              session: Session,
                              calculation_oid: int = None) \
        -> dm.LossCalculationBranch | dm.DamageCalculationBranch:

    if calculation_oid:
        branch['_calculation_oid'] = calculation_oid

    calculation_branch = CALCULATION_BRANCH_MAPPING[branch.pop(
        'calculation_mode')]

    calculation_branch = calculation_branch(**branch)
    session.add(calculation_branch)
    session.commit()

    return calculation_branch


def update_calculation_branch_status(calculation_oid: int,
                                     status: dm.EStatus,
                                     session: Session) -> dm.CalculationBranch:
    calculation = read_calculation_branch(calculation_oid, session)
    calculation.status = status
    session.commit()
    return calculation


def read_calculation(oid: int, session: Session) -> dm.Calculation:
    stmt = select(dm.Calculation).where(dm.Calculation._oid == oid)
    return session.execute(stmt).unique().scalar()


def update_calculation_status(calculation_oid: int,
                              status: dm.EStatus,
                              session: Session) -> dm.Calculation:
    calculation = read_calculation(calculation_oid, session)
    calculation.status = status
    session.commit()
    return calculation


def read_calculations(session: Session,
                      type: dm.EEarthquakeType | None) -> list[dm.Calculation]:

    stmt = select(dm.Calculation).order_by(dm.Calculation._oid)

    if type:
        stmt = stmt.where(dm.Calculation.riskassessment.has(
            dm.RiskAssessment.type == type))
    return session.execute(stmt).unique().scalars().all()


def delete_calculation(calculation_oid: int,
                       session: Session) -> None:
    stmt = delete(dm.Calculation).where(dm.Calculation._oid == calculation_oid)
    session.execute(stmt)
    session.commit()


def create_risk_values(risk_values: pd.DataFrame,
                       aggregation_tags: list[dm.AggregationTag],
                       connection):

    max_procs = int(os.getenv('MAX_PROCESSES', '1'))
    cursor = connection.cursor()

    # lock the table since we're setting indexes manually
    # TODO: find solution so it works with threading
    if max_procs == 1:
        cursor.execute(
            f'LOCK TABLE {dm.RiskValue.__table__.name} IN EXCLUSIVE MODE;')

    # create the index on the riskvalues
    index0 = get_nextval(cursor, dm.RiskValue.__table__.name, '_oid')

    risk_values['_oid'] = range(index0, index0 + len(risk_values))
    risk_values['losscategory'] = risk_values['losscategory'].map(
        attrgetter('name'))

    # build up many2many reference table dm.riskvalue_aggregationtag
    df_agg_val = pd.DataFrame(
        {'riskvalue': risk_values['_oid'],
         'aggregationtag': risk_values.pop('aggregationtags'),
         '_calculation_oid': risk_values['_calculation_oid'],
         'losscategory': risk_values['losscategory']})

    # Explode list of aggregationtags and replace with correct oid's
    df_agg_val = df_agg_val.explode('aggregationtag', ignore_index=True)
    df_agg_val['aggregationtype'] = df_agg_val['aggregationtag'].map(
        aggregation_tags).map(attrgetter('type'))
    df_agg_val['aggregationtag'] = df_agg_val['aggregationtag'].map(
        aggregation_tags).map(attrgetter('_oid'))

    if max_procs > 1:
        copy_pooled(risk_values, dm.RiskValue.__table__.name, max_procs)
        copy_pooled(df_agg_val, dm.riskvalue_aggregationtag.name, max_procs)
    else:
        copy_from_dataframe(cursor, risk_values, dm.RiskValue.__table__.name)
        copy_from_dataframe(
            cursor,
            df_agg_val,
            dm.riskvalue_aggregationtag.name)
    cursor.close()


def copy_pooled(df, tablename, max_procs, max_entries=750000):
    nprocs = 1
    while len(df) / nprocs > max_entries and nprocs < max_procs:
        nprocs += 1

    chunks = df.groupby(
        np.arange(len(df)) // (len(df) / nprocs))

    pool_args = [(chunk, tablename)
                 for _, chunk in chunks]

    with Pool(nprocs) as pool:
        pool.starmap(copy_raw, pool_args)


def copy_raw(df, tablename):
    connect_text = \
        f"dbname='{os.getenv('DB_NAME')}' " \
        f"user='{os.getenv('DB_USER')}' " \
        f"host={os.getenv('POSTGRES_HOST')} " \
        f"port={os.getenv('POSTGRES_PORT')} " \
        f"password='{os.getenv('DB_PASSWORD')}'"

    conn = psycopg2.connect(connect_text)
    cursor = conn.cursor()
    copy_from_dataframe(cursor, df, tablename)
    conn.commit()
    conn.close()


def read_aggregationtags(type: str,
                         exposuremodel_oid: int,
                         session: Session) -> list[dm.AggregationTag]:
    statement = select(dm.AggregationTag).where(
        (dm.AggregationTag.type == type)
        & (dm.AggregationTag._exposuremodel_oid == exposuremodel_oid))
    return session.execute(statement).unique().scalars().all()


def copy_from_dataframe(cursor, df: pd.DataFrame, table: str):
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, table, sep=",", columns=df.columns)
    except (Exception, psycopg2.DatabaseError) as err:
        cursor.close()
        raise err


def get_nextval(cursor, table: str, column: str):
    # set sequence to correct number
    cursor.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
        f"coalesce(max({column}),0) + 1, false) FROM {table};"
    )
    # get nextval
    cursor.execute(
        f"select nextval(pg_get_serial_sequence('{table}', '{column}'))")
    next = cursor.fetchone()[0]
    return next


def delete_risk_assessment(risk_assessment_oid: int,
                           session: Session) -> int:

    connection = session.get_bind().raw_connection()
    cursor = connection.cursor()

    stmt = select(dm.RiskAssessment).where(
        dm.RiskAssessment._oid == risk_assessment_oid)
    riskassessment = session.execute(stmt).unique().scalar()

    losscalculation_id = riskassessment._losscalculation_oid
    damagecalculation_id = riskassessment._damagecalculation_oid

    cursor.execute(
        "DELETE FROM loss_riskassessment WHERE _oid = {};".format(
            risk_assessment_oid))
    rowcount = cursor.rowcount

    if losscalculation_id:
        loss_table = f'loss_riskvalue_{losscalculation_id}'
        loss_assoc_table = f'loss_assoc_{losscalculation_id}'
        cursor.execute(
            "TRUNCATE TABLE {0} CASCADE;"
            "ALTER TABLE loss_riskvalue DETACH PARTITION {0};"
            "DROP TABLE {0};"
            "TRUNCATE TABLE {1};"
            "DROP TABLE {1};"
            "DELETE FROM loss_calculation WHERE _oid = {2};".format(
                loss_table, loss_assoc_table, losscalculation_id)
        )

    if damagecalculation_id:
        damage_table = f'loss_riskvalue_{damagecalculation_id}'
        damage_assoc_table = f'loss_assoc_{damagecalculation_id}'
        cursor.execute(
            "TRUNCATE TABLE {0} CASCADE;"
            "ALTER TABLE loss_riskvalue DETACH PARTITION {0};"
            "DROP TABLE {0};"
            "TRUNCATE TABLE {1};"
            "DROP TABLE {1};"
            "DELETE FROM loss_calculation WHERE _oid = {2};".format(
                damage_table, damage_assoc_table, damagecalculation_id)
        )

    connection.commit()
    cursor.close()
    connection.close()
    return rowcount


def create_risk_assessment(originid: str,
                           session: Session,
                           **kwargs
                           ) -> dm.RiskAssessment:

    risk_assessment = dm.RiskAssessment(
        originid=originid,
        **kwargs)
    session.add(risk_assessment)
    session.commit()

    return risk_assessment


def read_risk_assessments(
        session: Session,
        type: dm.EEarthquakeType | None = None) -> list[dm.RiskAssessment]:

    stmt = select(dm.RiskAssessment)
    if type:
        stmt = stmt.where(dm.RiskAssessment.type == type)
    return session.execute(stmt).unique().scalars().all()


def read_risk_assessment(oid: int, session: Session) -> dm.RiskAssessment:
    stmt = select(dm.RiskAssessment).where(dm.RiskAssessment._oid == oid)
    return session.execute(stmt).unique().scalar()


def update_risk_assessment_status(riskassessment_oid: int,
                                  status: dm.EStatus,
                                  session: Session) -> dm.Calculation:
    risk_assessment = read_risk_assessment(riskassessment_oid, session)
    risk_assessment.status = status
    session.commit()
    return risk_assessment
