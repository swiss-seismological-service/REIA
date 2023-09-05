import configparser
from pathlib import Path

import pytest

from reia.actions import run_openquake_calculations
from reia.cli import (add_exposure, add_fragility, add_risk_assessment,
                      add_taxonomymap, add_vulnerability)
from reia.db import crud, session
from reia.io import CalculationBranchSettings


@pytest.fixture(scope='module')
def db_session():
    yield session
    session.remove()


@pytest.fixture(scope='module')
def riskassessment(db_session):
    datafolder = Path(__file__).parent / 'data' / 'ria_test'

    risk_file = configparser.ConfigParser()
    risk_file.read(str(datafolder / 'risk.ini'))

    damage_file = configparser.ConfigParser()
    damage_file.read(str(datafolder / 'damage.ini'))

    exposure_id = add_exposure(datafolder / 'exposure_test.xml', 'test')
    fragility_id = add_fragility(datafolder / 'fragility_test.xml', 'test')
    taxonomy_id = add_taxonomymap(datafolder / 'taxonomy_test.csv', 'test')
    vulnerability_id = add_vulnerability(
        datafolder / 'vulnerability_test.xml', 'test')

    # risk
    risk_file['exposure']['exposure_file'] = str(exposure_id)
    risk_file['vulnerability']['structural_vulnerability_file'] = str(
        vulnerability_id)

    risk_file['hazard']['gmfs_csv'] = str(datafolder / 'gmfs_test.csv')
    risk_file['hazard']['sites_csv'] = str(datafolder / 'sites.csv')

    settings = [CalculationBranchSettings(1, risk_file)]

    losscalculation = run_openquake_calculations(settings, db_session)

    # damage
    damage_file['exposure']['exposure_file'] = str(exposure_id)
    damage_file['fragility']['structural_fragility_file'] = str(fragility_id)
    damage_file['fragility']['taxonomy_mapping_csv'] = str(taxonomy_id)

    damage_file['hazard']['gmfs_csv'] = str(datafolder / 'gmfs_test.csv')
    damage_file['hazard']['sites_csv'] = str(datafolder / 'sites.csv')

    settings = [CalculationBranchSettings(1, damage_file)]

    damagecalculation = run_openquake_calculations(settings, db_session)

    riskassessment_id = add_risk_assessment(
        'smi:ch.ethz.sed/test', losscalculation._oid, damagecalculation._oid)

    return crud.read_risk_assessment(riskassessment_id, db_session)


def test_riskassessment(riskassessment, db_session):
    assert riskassessment.originid == 'smi:ch.ethz.sed/test'
