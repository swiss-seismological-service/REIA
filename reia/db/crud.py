from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

import reia.datamodel as dm
from reia.io import CALCULATION_BRANCH_MAPPING, CALCULATION_MAPPING


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
