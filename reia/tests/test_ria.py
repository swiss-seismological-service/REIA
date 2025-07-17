import configparser
from pathlib import Path

import pytest
from numpy.testing import assert_almost_equal

from reia.actions import run_openquake_calculations
from reia.cli import (add_exposure, add_fragility, add_risk_assessment,
                      add_taxonomymap, add_vulnerability)
from reia.datamodel import ECalculationType, EStatus
from reia.db import crud
from reia.io import CalculationBranchSettings
from reia.repositories.fragility import TaxonomyMapRepository

DATAFOLDER = Path(__file__).parent / 'data' / 'ria_test'


@pytest.fixture(scope='module')
def exposure(db_session):
    exposure_id = add_exposure(DATAFOLDER / 'exposure_test.xml', 'test')
    return crud.read_asset_collection(exposure_id, db_session)


@pytest.fixture(scope='module')
def fragility(db_session):
    fragility_id = add_fragility(DATAFOLDER / 'fragility_test.xml', 'test')
    return crud.read_fragility_model(fragility_id, db_session)


@pytest.fixture(scope='module')
def taxonomy(db_session):
    taxonomy_id = add_taxonomymap(DATAFOLDER / 'taxonomy_test.csv', 'test')
    return TaxonomyMapRepository.get_by_id(db_session, taxonomy_id)


@pytest.fixture(scope='module')
def vulnerability(db_session):
    vulnerability_id = add_vulnerability(
        DATAFOLDER / 'vulnerability_test.xml', 'test')
    return crud.read_vulnerability_model(vulnerability_id, db_session)


@pytest.fixture(scope='module')
def loss_calculation(exposure, vulnerability, db_session):
    risk_file = configparser.ConfigParser()
    risk_file.read(str(DATAFOLDER / 'risk.ini'))

    # risk
    risk_file['exposure']['exposure_file'] = str(exposure._oid)
    risk_file['vulnerability']['structural_vulnerability_file'] = str(
        vulnerability._oid)

    risk_file['hazard']['gmfs_csv'] = str(DATAFOLDER / 'gmfs_test.csv')
    risk_file['hazard']['sites_csv'] = str(DATAFOLDER / 'sites.csv')

    settings = [CalculationBranchSettings(1, risk_file)]

    return run_openquake_calculations(settings, db_session)


@pytest.fixture(scope='module')
def damage_calculation(exposure, fragility, taxonomy, db_session):
    damage_file = configparser.ConfigParser()
    damage_file.read(str(DATAFOLDER / 'damage.ini'))

    # damage
    damage_file['exposure']['exposure_file'] = str(exposure._oid)
    damage_file['fragility']['structural_fragility_file'] = str(fragility._oid)
    damage_file['fragility']['taxonomy_mapping_csv'] = str(taxonomy.oid)

    damage_file['hazard']['gmfs_csv'] = str(DATAFOLDER / 'gmfs_test.csv')
    damage_file['hazard']['sites_csv'] = str(DATAFOLDER / 'sites.csv')

    settings = [CalculationBranchSettings(1, damage_file)]

    return run_openquake_calculations(settings, db_session)


@pytest.fixture(scope='module')
def risk_assessment(loss_calculation, damage_calculation, db_session):
    riskassessment_id = add_risk_assessment(
        'smi:ch.ethz.sed/test', loss_calculation._oid, damage_calculation._oid)
    return crud.read_risk_assessment(riskassessment_id, db_session)


def test_riskassessment(risk_assessment, loss_calculation, damage_calculation):
    assert risk_assessment.originid == 'smi:ch.ethz.sed/test'
    assert risk_assessment._losscalculation_oid == loss_calculation._oid
    assert risk_assessment._damagecalculation_oid == damage_calculation._oid


def test_calculations(loss_calculation, damage_calculation):
    aggregateby = ['Canton; CantonGemeinde']
    assert loss_calculation.aggregateby == \
        damage_calculation.aggregateby == aggregateby
    assert loss_calculation.status == \
        damage_calculation.status == EStatus.COMPLETE
    assert loss_calculation._type == ECalculationType.LOSS
    assert damage_calculation._type == ECalculationType.DAMAGE


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

    assert loss_branch._exposuremodel_oid == \
        damage_branch._exposuremodel_oid == exposure._oid

    assert loss_branch._structuralvulnerabilitymodel_oid == vulnerability._oid
    assert loss_branch._type == ECalculationType.LOSS
    assert loss_branch._contentsvulnerabilitymodel_oid == \
        loss_branch._occupantsvulnerabilitymodel_oid == \
        loss_branch._nonstructuralvulnerabilitymodel_oid == \
        loss_branch._businessinterruptionvulnerabilitymodel_oid is None

    assert damage_branch._structuralfragilitymodel_oid == fragility._oid
    assert damage_branch._taxonomymap_oid == taxonomy.oid
    assert damage_branch._type == ECalculationType.DAMAGE
    assert damage_branch._contentsfragilitymodel_oid == \
        damage_branch._nonstructuralfragilitymodel_oid == \
        damage_branch._businessinterruptionfragilitymodel_oid is None


def test_riskvalues(loss_calculation, damage_calculation):
    losses = loss_calculation.losses
    damages = damage_calculation.damages
    assert len(losses) == 12
    assert len(damages) == 26


def test_aggreagationtags(loss_calculation, damage_calculation, exposure):
    losses = loss_calculation.losses
    aggregationtags = set(
        [tag for loss in losses for tag in loss.aggregationtags])
    assert len(aggregationtags) == 2
    assert all(tag._exposuremodel_oid == exposure._oid
               for tag in aggregationtags)


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

    assert set([a._site_oid for a in assets]).issubset(
        set([s._oid for s in sites]))

    assert_almost_equal(
        sum([a.structuralvalue for a in assets]) / len(assets),
        2357478.8205128205, 4)

    assert_almost_equal(
        sum([s.longitude for s in sites]) / len(sites),
        9.51877649849495, 4)
    assert_almost_equal(
        sum([s.latitude for s in sites]) / len(sites),
        46.86077451544124, 4)

    aggregationtags = set(
        [tag for asset in assets for tag in asset.aggregationtags])
    assert len(aggregationtags) == 4
    assert all(tag._exposuremodel_oid == exposure._oid
               for tag in aggregationtags)


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
