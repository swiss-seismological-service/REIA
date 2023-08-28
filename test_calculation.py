from sqlalchemy import select

from reia.datamodel import (AggregationTag, ELossCategory, EStatus,
                            ExposureModel, LossCalculation,
                            LossCalculationBranch, LossValue, RiskAssessment,
                            StructuralVulnerabilityModel)
from reia.db import session

loss_calculation = LossCalculation(
    status=EStatus.COMPLETE,
    aggregateby=['Canton'])

risk_assessment = RiskAssessment(
    originid='test',
    status=EStatus.COMPLETE,
    type='NATURAL',
    preferred=False,
    published=False,
    losscalculation=loss_calculation)

stmt = select(ExposureModel)
exposure_model = session.execute(stmt).unique().scalars().first()
stmt = select(StructuralVulnerabilityModel)
structural_vulnerability_model = session.execute(
    stmt).unique().scalars().first()

loss_calculation_branch = LossCalculationBranch(
    status=EStatus.COMPLETE,
    weight=1,
    exposuremodel=exposure_model,
    losscalculation=loss_calculation,
    structuralvulnerabilitymodel=structural_vulnerability_model)

stmt = select(AggregationTag).where(AggregationTag.name == 'GE')
tag = session.execute(stmt).unique().scalars().all()

loss_value_1 = LossValue(losscategory=ELossCategory.STRUCTURAL,
                         eventid=1,
                         weight=1,
                         losscalculation=loss_calculation,
                         losscalculationbranch=loss_calculation_branch,
                         loss_value=100,
                         aggregationtags=tag)

session.add(loss_calculation)
session.add(risk_assessment)
session.add(loss_calculation_branch)
session.commit()
