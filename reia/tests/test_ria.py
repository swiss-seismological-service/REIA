import pytest
from numpy.testing import assert_almost_equal

from reia.repositories.asset import (AggregationTagRepository, AssetRepository,
                                     SiteRepository)
from reia.repositories.calculation import (CalculationBranchRepository,
                                           CalculationRepository,
                                           RiskAssessmentRepository)
from reia.schemas.enums import ECalculationType, EStatus
from reia.services.riskassessment import RiskAssessmentService


def test_run_risk_assessment_end_to_end(
        loss_config, damage_config, db_session):
    """Test the full run_risk_assessment CLI workflow end-to-end."""

    # Call the actual CLI function
    originid = 'smi:ch.ethz.sed/e2e_test'
    service = RiskAssessmentService(db_session)
    risk_assessment = service.run_risk_assessment(
        originid, loss_config, damage_config)

    # Verify the risk assessment was created properly
    e2e_assessment = RiskAssessmentRepository.get_by_id(
        db_session, risk_assessment.oid)

    assert e2e_assessment is not None, \
        f"Risk assessment with originid {originid} not found"
    assert e2e_assessment.status == EStatus.COMPLETE
    assert e2e_assessment.losscalculation_oid is not None
    assert e2e_assessment.damagecalculation_oid is not None

    # Verify both calculations completed successfully
    loss_calc = CalculationRepository.get_by_id(
        db_session, e2e_assessment.losscalculation_oid)
    damage_calc = CalculationRepository.get_by_id(
        db_session, e2e_assessment.damagecalculation_oid)

    assert loss_calc.status == EStatus.COMPLETE
    assert damage_calc.status == EStatus.COMPLETE


def test_riskassessment(risk_assessment, loss_calculation, damage_calculation):
    assert risk_assessment.originid == 'smi:ch.ethz.sed/test'
    assert risk_assessment.losscalculation_oid == loss_calculation.oid
    assert risk_assessment.damagecalculation_oid == damage_calculation.oid


def test_calculations(loss_calculation, damage_calculation):
    aggregateby = ['Canton; CantonGemeinde']
    assert loss_calculation.aggregateby == \
        damage_calculation.aggregateby == aggregateby
    assert loss_calculation.status == \
        damage_calculation.status == EStatus.COMPLETE
    assert loss_calculation.type == ECalculationType.LOSS
    assert damage_calculation.type == ECalculationType.DAMAGE


def test_calculationbranches(loss_calculation,
                             damage_calculation,
                             exposure,
                             vulnerability,
                             fragility,
                             taxonomy):
    assert len(loss_calculation.losscalculationbranches) == 1
    loss_branch = loss_calculation.losscalculationbranches[0]

    assert len(damage_calculation.damagecalculationbranches) == 1
    damage_branch = damage_calculation.damagecalculationbranches[0]

    assert loss_branch.status == damage_branch.status == EStatus.COMPLETE
    assert loss_branch.weight == damage_branch.weight == 1.0

    assert loss_branch.exposuremodel_oid == \
        damage_branch.exposuremodel_oid == exposure.oid

    assert loss_branch.structuralvulnerabilitymodel_oid == vulnerability.oid
    assert loss_branch.type == ECalculationType.LOSS
    assert loss_branch.contentsvulnerabilitymodel_oid == \
        loss_branch.occupantsvulnerabilitymodel_oid == \
        loss_branch.nonstructuralvulnerabilitymodel_oid == \
        loss_branch.businessinterruptionvulnerabilitymodel_oid is None

    assert damage_branch.structuralfragilitymodel_oid == fragility.oid
    assert damage_branch.taxonomymap_oid == taxonomy.oid
    assert damage_branch.type == ECalculationType.DAMAGE
    assert damage_branch.contentsfragilitymodel_oid == \
        damage_branch.nonstructuralfragilitymodel_oid == \
        damage_branch.businessinterruptionfragilitymodel_oid is None


def test_riskvalues(loss_calculation, damage_calculation):
    losses = loss_calculation.losses
    damages = damage_calculation.damages
    assert len(losses) == 12
    assert len(damages) == 26


def test_aggregationtags(loss_calculation, damage_calculation, exposure):
    losses = loss_calculation.losses
    aggregationtags = set([tag.oid for loss in losses
                           for tag in loss.aggregationtags])
    assert len(aggregationtags) == 2

    exposuremodel_oid = set([tag.exposuremodel_oid for loss in losses
                             for tag in loss.aggregationtags])
    assert len(exposuremodel_oid) == 1


def test_exposuremodel(exposure, db_session):

    assert exposure.name == 'test'
    assert exposure.category == 'buildings'
    assert exposure.aggregationtypes == ['Canton', 'CantonGemeinde']
    assert exposure.dayoccupancy == exposure.nightoccupancy == \
        exposure.transitoccupancy is True

    aggregationtags = AggregationTagRepository.get_by_exposuremodel(
        db_session, exposure.oid)
    assert len(aggregationtags) == 4
    assert len(exposure.costtypes) == 4
    assert 'structural' in [cost.name for cost in exposure.costtypes]


def test_assets(exposure, db_session):
    assets = AssetRepository.get_by_exposuremodel(db_session, exposure.oid)
    sites = SiteRepository.get_by_exposuremodel(db_session, exposure.oid)

    assert len(assets) == 39
    assert len(sites) == 28
    print(assets.columns)
    assert set(assets['_site_oid'].to_list()).issubset(
        set([s.oid for s in sites]))

    assert_almost_equal(
        assets['structuralvalue'].sum() / len(assets),
        2357478.8205128205, 4)

    assert_almost_equal(
        sum([s.longitude for s in sites]) / len(sites),
        9.51877649849495, 4)
    assert_almost_equal(
        sum([s.latitude for s in sites]) / len(sites),
        46.86077451544124, 4)

    aggregationtags = AggregationTagRepository.get_by_exposuremodel(
        db_session, exposure.oid)
    assert len(aggregationtags) == 4

    exposuremodel_oids = set([tag.exposuremodel_oid for
                              tag in aggregationtags])
    assert len(exposuremodel_oids) == 1


def test_loss_results(loss_calculation):
    losses = loss_calculation.losses

    loss_canton = sum([ls.loss_value * ls.weight for ls in losses
                       if ls.aggregationtags[0].type == 'Canton'])
    loss_gemeinde = sum([ls.loss_value * ls.weight for ls in losses
                         if ls.aggregationtags[0].type == 'CantonGemeinde'])
    assert_almost_equal(loss_canton, 325.357, 2)
    assert_almost_equal(loss_gemeinde, 325.357, 2)

    loss_canton_gr = sum([ls.loss_value * ls.weight for ls in losses
                          if ls.aggregationtags[0].type == 'Canton'
                          and ls.aggregationtags[0].name == 'GR'])
    assert_almost_equal(loss_canton_gr, 325.357, 2)


def test_damage_results(damage_calculation):
    damages = damage_calculation.damages

    dg1_canton = sum([dg.dg1_value * dg.weight for dg in damages
                      if dg.aggregationtags[0].type == 'Canton'])
    dg1_gemeinde = sum([dg.dg1_value * dg.weight for dg in damages
                        if dg.aggregationtags[0].type == 'CantonGemeinde'])

    dg2_canton = sum([dg.dg2_value * dg.weight for dg in damages
                      if dg.aggregationtags[0].type == 'Canton'])
    dg3_canton = sum([dg.dg3_value * dg.weight for dg in damages
                      if dg.aggregationtags[0].type == 'Canton'])
    dg4_canton = sum([dg.dg4_value * dg.weight for dg in damages
                      if dg.aggregationtags[0].type == 'Canton'])
    dg5_canton = sum([dg.dg5_value * dg.weight for dg in damages
                      if dg.aggregationtags[0].type == 'Canton'])

    assert_almost_equal(dg1_canton, 2.73759E-03, 5)
    assert_almost_equal(dg1_gemeinde, 2.73759E-03, 5)
    assert_almost_equal(dg2_canton, 6.57239E-04, 5)
    assert_almost_equal(dg3_canton, 1.19269E-04, 5)
    assert_almost_equal(dg4_canton, 0.00273759E-03, 5)
    assert_almost_equal(dg5_canton, 5.86482E-07, 5)

    dg1_canton_gr = sum([dg.dg1_value * dg.weight for dg in damages
                         if dg.aggregationtags[0].type == 'Canton'
                         and dg.aggregationtags[0].name == 'GR'])
    assert_almost_equal(dg1_canton_gr, 2.73759E-03, 5)


def test_get_calculation_branches(
        db_session,
        loss_calculation,
        damage_calculation):
    loss_branch = CalculationBranchRepository.get_by_id(
        db_session, loss_calculation.losscalculationbranches[0].oid)
    damage_branch = CalculationBranchRepository.get_by_id(
        db_session, damage_calculation.damagecalculationbranches[0].oid)

    assert loss_branch is not None
    assert loss_branch.type == ECalculationType.LOSS
    assert damage_branch is not None
    assert damage_branch.type == ECalculationType.DAMAGE


def test_get_calculation_by_type(db_session):
    loss_calc = CalculationRepository.get_all_by_type(
        db_session, ECalculationType.LOSS)
    damage_calc = CalculationRepository.get_all_by_type(
        db_session, ECalculationType.DAMAGE)

    assert all(calc.type == ECalculationType.LOSS for calc in loss_calc)
    assert all(calc.type == ECalculationType.DAMAGE for calc in damage_calc)


def test_delete_risk_assessment(risk_assessment, db_session):
    """Test the deletion of a risk assessment."""
    riskassessment_oid = risk_assessment.oid

    # Delete the risk assessment
    deleted_count = RiskAssessmentRepository.delete(
        db_session, riskassessment_oid)

    assert deleted_count == 1, "Risk assessment should be deleted"

    # Verify it no longer exists
    deleted_assessment = RiskAssessmentRepository.get_by_id(
        db_session, riskassessment_oid)
    assert deleted_assessment is None, \
        f"Risk assessment {riskassessment_oid} should not exist after deletion"


def test_update_risk_assessment_status(risk_assessment, db_session):
    """Test updating the status of a risk assessment."""
    riskassessment_oid = risk_assessment.oid

    with pytest.raises(ValueError):
        # Attempt to update a non-existent risk assessment
        RiskAssessmentRepository.update_risk_assessment_status(
            db_session, riskassessment_oid, EStatus.COMPLETE)
