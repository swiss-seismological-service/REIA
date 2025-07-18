from uuid import UUID

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

import reia.datamodel as dm
from reia.db.copy import allocate_oids, copy_pooled, db_cursor_from_session
from reia.io import (CALCULATION_BRANCH_MAPPING, CALCULATION_MAPPING,
                     LOSSCATEGORY_VULNERABILITY_MAPPING)


def create_vulnerability_model(
    model: dict,
    session: Session) \
    -> dm.StructuralVulnerabilityModel | \
        dm.OccupantsVulnerabilityModel | \
        dm.NonstructuralVulnerabilityModel | \
        dm.BusinessInterruptionVulnerabilityModel | \
        dm.ContentsVulnerabilityModel:
    """Creates a vulnerabilitymodel of the right subtype.

    Creates a vulnerabilitymodel of the right subtype from a dict containing
    all the data.

    Args:
        model: Dictionary containing vulnerability model data.
        session: Database session object.

    Returns:
        The created vulnerability model instance.
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


def read_sites(asset_collection_oid: int, session: Session) -> list[dm.Site]:
    stmt = select(dm.Site).where(
        dm.Site._exposuremodel_oid == asset_collection_oid)
    return session.execute(stmt).scalars().all()


def read_asset_collection(oid, session: Session) -> dm.ExposureModel:
    stmt = select(dm.ExposureModel).where(dm.ExposureModel._oid == oid)
    return session.execute(stmt).unique().scalar()


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


def create_risk_values(risk_values: pd.DataFrame,
                       df_agg_val: pd.DataFrame,
                       session: Session) -> None:

    with db_cursor_from_session(session) as cursor:
        db_indexes = allocate_oids(cursor,
                                   dm.RiskValue.__table__.name,
                                   '_oid',
                                   len(risk_values))

    oid_map = dict(zip(risk_values['_oid'], db_indexes))

    risk_values['_oid'] = db_indexes
    df_agg_val['riskvalue'] = df_agg_val['riskvalue'].map(oid_map)

    copy_pooled(risk_values, dm.RiskValue.__table__.name)
    copy_pooled(df_agg_val, dm.riskvalue_aggregationtag.name)


def delete_risk_assessment(risk_assessment_oid: UUID,
                           session: Session) -> int:

    connection = session.get_bind().raw_connection()
    cursor = connection.cursor()

    stmt = select(dm.RiskAssessment).where(
        dm.RiskAssessment._oid == risk_assessment_oid)
    riskassessment = session.execute(stmt).unique().scalar()

    if not riskassessment:
        return 0

    losscalculation_id = riskassessment._losscalculation_oid
    damagecalculation_id = riskassessment._damagecalculation_oid

    cursor.execute(
        "DELETE FROM loss_riskassessment WHERE _oid = {};".format(
            f"'{str(risk_assessment_oid)}'"))
    rowcount = cursor.rowcount
    connection.commit()

    if losscalculation_id:

        loss_assoc_table = f'loss_assoc_{losscalculation_id}'
        cursor.execute(
            "TRUNCATE TABLE {0};"
            "DROP TABLE {0};".format(
                loss_assoc_table))
        connection.commit()

        loss_table = f'loss_riskvalue_{losscalculation_id}'
        cursor.execute(
            "ALTER TABLE loss_riskvalue DETACH PARTITION {0};"
            "TRUNCATE TABLE {0};"
            "DROP TABLE {0};"
            "DELETE FROM loss_calculation WHERE _oid = {1};".format(
                loss_table,
                losscalculation_id))

    if damagecalculation_id:
        damage_assoc_table = f'loss_assoc_{damagecalculation_id}'
        cursor.execute(
            "TRUNCATE TABLE {0};"
            "DROP TABLE {0};".format(
                damage_assoc_table))
        connection.commit()

        damage_table = f'loss_riskvalue_{damagecalculation_id}'
        cursor.execute(
            "ALTER TABLE loss_riskvalue DETACH PARTITION {0};"
            "TRUNCATE TABLE {0};"
            "DROP TABLE {0};"
            "DELETE FROM loss_calculation WHERE _oid = {1};".format(
                damage_table,
                damagecalculation_id))

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


def read_risk_assessment(oid: UUID, session: Session) -> dm.RiskAssessment:
    stmt = select(dm.RiskAssessment).where(dm.RiskAssessment._oid == oid)
    return session.execute(stmt).unique().scalar()


def update_risk_assessment_status(riskassessment_oid: UUID,
                                  status: dm.EStatus,
                                  session: Session) -> dm.Calculation:
    risk_assessment = read_risk_assessment(riskassessment_oid, session)
    risk_assessment.status = status
    session.commit()
    return risk_assessment
