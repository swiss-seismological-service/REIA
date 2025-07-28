import configparser
from pathlib import Path

import pytest
from numpy.testing import assert_almost_equal

from reia.services.calculation import CalculationService
from reia.cli import (add_exposure, add_fragility, add_risk_assessment,
                      add_taxonomymap, add_vulnerability, run_risk_assessment)
from reia.repositories.asset import ExposureModelRepository
from reia.repositories.calculation import (CalculationRepository,
                                           RiskAssessmentRepository)
from reia.repositories.fragility import (FragilityModelRepository,
                                         TaxonomyMapRepository)
from reia.repositories.vulnerability import VulnerabilityModelRepository
from reia.schemas.calculation_schemas import CalculationBranchSettings
from reia.schemas.enums import ECalculationType, EStatus

DATAFOLDER = Path(__file__).parent / 'data' / 'ria_test'


@pytest.fixture(scope='module')
def exposure(db_session):
    exposure_id = add_exposure(DATAFOLDER / 'exposure_test.xml', 'test')
    return ExposureModelRepository.get_by_id(db_session, exposure_id)


@pytest.fixture(scope='module')
def fragility(db_session):
    fragility_id = add_fragility(DATAFOLDER / 'fragility_test.xml', 'test')
    return FragilityModelRepository.get_by_id(db_session, fragility_id)


@pytest.fixture(scope='module')
def taxonomy(db_session):
    taxonomy_id = add_taxonomymap(DATAFOLDER / 'taxonomy_test.csv', 'test')
    return TaxonomyMapRepository.get_by_id(db_session, taxonomy_id)


@pytest.fixture(scope='module')
def vulnerability(db_session):
    vulnerability_id = add_vulnerability(
        DATAFOLDER / 'vulnerability_test.xml', 'test')
    return VulnerabilityModelRepository.get_by_id(db_session, vulnerability_id)


@pytest.fixture(scope='module')
def loss_config(exposure, vulnerability):
    """Create loss calculation config."""
    risk_file = configparser.ConfigParser()
    risk_file.read(str(DATAFOLDER / 'risk.ini'))

    risk_file['exposure']['exposure_file'] = str(exposure.oid)
    risk_file['vulnerability']['structural_vulnerability_file'] = str(
        vulnerability.oid)

    risk_file['hazard']['gmfs_csv'] = str(DATAFOLDER / 'gmfs_test.csv')
    risk_file['hazard']['sites_csv'] = str(DATAFOLDER / 'sites.csv')

    return risk_file


@pytest.fixture(scope='module')
def loss_calculation(loss_config, db_session):
    settings = [CalculationBranchSettings(weight=1, config=loss_config)]
    calc_service = CalculationService(db_session)
    return calc_service.run_calculations(settings)


@pytest.fixture(scope='module')
def damage_config(exposure, fragility, taxonomy):
    """Create damage calculation config."""
    damage_file = configparser.ConfigParser()
    damage_file.read(str(DATAFOLDER / 'damage.ini'))

    damage_file['exposure']['exposure_file'] = str(exposure.oid)
    damage_file['fragility']['structural_fragility_file'] = str(fragility.oid)
    damage_file['fragility']['taxonomy_mapping_csv'] = str(taxonomy.oid)

    damage_file['hazard']['gmfs_csv'] = str(DATAFOLDER / 'gmfs_test.csv')
    damage_file['hazard']['sites_csv'] = str(DATAFOLDER / 'sites.csv')

    return damage_file


@pytest.fixture(scope='module')
def damage_calculation(damage_config, db_session):
    settings = [CalculationBranchSettings(weight=1, config=damage_config)]
    calc_service = CalculationService(db_session)
    return calc_service.run_calculations(settings)


@pytest.fixture(scope='module')
def risk_assessment(loss_calculation, damage_calculation, db_session):
    riskassessment_id = add_risk_assessment(
        'smi:ch.ethz.sed/test', loss_calculation.oid, damage_calculation.oid)
    return RiskAssessmentRepository.get_by_id(db_session, riskassessment_id)


def test_run_risk_assessment_end_to_end(
        loss_config, damage_config, db_session):
    """Test the full run_risk_assessment CLI workflow end-to-end."""
    import os
    import tempfile

    # Create temporary config files
    with tempfile.NamedTemporaryFile(mode='w',
                                     suffix='.ini',
                                     delete=False) as loss_file:
        loss_config.write(loss_file)
        loss_file_path = loss_file.name

    with tempfile.NamedTemporaryFile(mode='w',
                                     suffix='.ini',
                                     delete=False) as damage_file:
        damage_config.write(damage_file)
        damage_file_path = damage_file.name

    try:
        # Call the actual CLI function
        originid = 'smi:ch.ethz.sed/e2e_test'
        run_risk_assessment(
            originid=originid,
            loss=loss_file_path,
            damage=damage_file_path
        )

        # Verify the risk assessment was created properly
        risk_assessments = RiskAssessmentRepository.get_all(db_session)
        e2e_assessment = None
        for ra in risk_assessments:
            if ra.originid == originid:
                e2e_assessment = ra
                break

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

    finally:
        # Clean up temporary files
        os.unlink(loss_file_path)
        os.unlink(damage_file_path)


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


def test_exposuremodel(exposure):

    assert exposure.name == 'test'
    assert exposure.category == 'buildings'
    assert exposure.aggregationtypes == ['Canton', 'CantonGemeinde']
    assert exposure.dayoccupancy == exposure.nightoccupancy == \
        exposure.transitoccupancy is True

    assert len(exposure.aggregationtags) == 4
    assert len(exposure.costtypes) == 4
    assert 'structural' in [cost.name for cost in exposure.costtypes]


def test_assets(exposure):
    assets = exposure.assets
    sites = exposure.sites

    assert len(assets) == 39
    assert len(exposure.sites) == 28

    assert set([a.site_oid for a in assets]).issubset(
        set([s.oid for s in sites]))

    assert_almost_equal(
        sum([a.structuralvalue for a in assets]) / len(assets),
        2357478.8205128205, 4)

    assert_almost_equal(
        sum([s.longitude for s in sites]) / len(sites),
        9.51877649849495, 4)
    assert_almost_equal(
        sum([s.latitude for s in sites]) / len(sites),
        46.86077451544124, 4)

    aggregationtags = set([tag.oid for asset in assets
                           for tag in asset.aggregationtags])
    assert len(aggregationtags) == 4

    exposuremodel_oid = set([tag.exposuremodel_oid for asset in assets
                             for tag in asset.aggregationtags])
    assert len(exposuremodel_oid) == 1


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
