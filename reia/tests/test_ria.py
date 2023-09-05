import configparser
from pathlib import Path

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from reia.actions import run_openquake_calculations
from reia.cli import (add_exposure, add_fragility, add_risk_assessment,
                      add_taxonomymap, add_vulnerability)
from reia.db import crud, engine
from reia.io import CalculationBranchSettings

DATAFOLDER = Path(__file__).parent / 'data' / 'ria_test'


@pytest.fixture(scope='module')
def db_session():
    session = scoped_session(sessionmaker(autocommit=False,
                                          bind=engine,
                                          future=True))
    yield session
    session.remove()


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
    return crud.read_taxonomymap(taxonomy_id, db_session)


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
    damage_file['fragility']['taxonomy_mapping_csv'] = str(taxonomy._oid)

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
