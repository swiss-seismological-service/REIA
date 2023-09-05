import configparser
from pathlib import Path

from reia.actions import run_openquake_calculations
from reia.cli import (add_exposure, add_fragility, add_risk_assessment,
                      add_taxonomymap, add_vulnerability)
from reia.db import session
from reia.io import CalculationBranchSettings


def test_ria():
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

    losscalculation = run_openquake_calculations(settings, session)

    # damage
    damage_file['exposure']['exposure_file'] = str(exposure_id)
    damage_file['fragility']['structural_fragility_file'] = str(fragility_id)
    damage_file['fragility']['taxonomy_mapping_csv'] = str(taxonomy_id)

    damage_file['hazard']['gmfs_csv'] = str(datafolder / 'gmfs_test.csv')
    damage_file['hazard']['sites_csv'] = str(datafolder / 'sites.csv')

    settings = [CalculationBranchSettings(1, damage_file)]

    damagecalculation = run_openquake_calculations(settings, session)

    riskassessment = add_risk_assessment(
        'smi:ch.ethz.sed/test', losscalculation._oid, damagecalculation._oid)

    print(riskassessment)

    session.remove()
