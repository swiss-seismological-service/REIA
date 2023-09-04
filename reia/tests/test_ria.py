import configparser
import os

from reia.actions import (dispatch_openquake_calculation,
                          run_openquake_calculations)
from reia.cli import (add_exposure, add_fragility, add_taxonomymap,
                      add_vulnerability)
from reia.db import session
from reia.io import CalculationBranchSettings


def test_ria():
    folder = os.path.dirname(os.path.abspath(__file__))
    datafolder = os.path.join(folder, 'data', 'ria_test')

    gmfs = os.path.join(datafolder, 'gmfs_test.csv')
    sites = os.path.join(datafolder, 'sites.csv')
    exposure = os.path.join(datafolder, 'exposure_test.xml')
    fragility = os.path.join(datafolder, 'fragility_test.xml')
    taxonomy = os.path.join(datafolder, 'taxonomy_mapping_test.csv')
    vulnerability = os.path.join(
        datafolder, 'structural_vulnerability_test.xml')

    risk_file = configparser.ConfigParser()
    risk_file.read(os.path.join(datafolder, 'risk.ini'))
    damage_file = configparser.ConfigParser()
    damage_file.read(os.path.join(datafolder, 'damage.ini'))

    exposure_id = add_exposure(exposure, 'test')
    fragility_id = add_fragility(fragility, 'test')
    taxonomy_id = add_taxonomymap(taxonomy, 'test')
    vulnerability_id = add_vulnerability(vulnerability, 'test')

    # risk
    risk_file['exposure']['exposure_file'] = str(exposure_id)
    risk_file['vulnerability']['structural_vulnerability_file'] = str(
        vulnerability_id)
    risk_file['hazard']['gmfs_csv'] = gmfs
    risk_file['hazard']['sites_csv'] = sites

    settings = [CalculationBranchSettings(1, risk_file)]

    run_openquake_calculations(settings, session)

    # response = dispatch_openquake_calculation(risk_file, session)

    # print(response)

    # # damage
    # damage_file['exposure']['exposure_file'] = str(exposure_id)
    # damage_file['fragility']['structural_fragility_file'] = str(fragility_id)
    # damage_file['fragility']['taxonomy_mapping_csv'] = str(taxonomy_id)
    # damage_file['hazard']['gmfs_csv'] = gmfs
    # damage_file['hazard']['sites_csv'] = sites
    # response = dispatch_openquake_calculation(damage_file, session)

    # print(response)

    session.remove()
