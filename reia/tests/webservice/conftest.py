import configparser
import tempfile
from pathlib import Path

import pytest

from reia.repositories.calculation import RiskAssessmentRepository
from reia.schemas.calculation_schemas import RiskAssessment
from reia.schemas.enums import ECalculationType
from reia.services.calculation import (CalculationDataService,
                                       CalculationExecutionService)
from reia.services.creation_info import populate_creation_info
from reia.services.exposure import ExposureService
from reia.services.fragility import FragilityService
from reia.services.taxonomy import TaxonomyService
from reia.services.vulnerability import VulnerabilityService

CALCULATION = Path(__file__).parent / 'data' / 'calculation'


@pytest.fixture(scope='module')
def example_exposure(db_session):
    """Import example exposure model."""
    exposure = ExposureService.import_from_files(
        db_session,
        CALCULATION / 'exposure_model_converted.xml',
        'example_exposure')
    return exposure


@pytest.fixture(scope='module')
def example_fragility(db_session):
    """Import example fragility model."""
    fragility = FragilityService.import_from_files(
        db_session,
        CALCULATION / 'structural_fragility_model.xml',
        'example_fragility')
    return fragility


@pytest.fixture(scope='module')
def example_taxonomy(db_session):
    """Import example taxonomy mapping."""
    taxonomy = TaxonomyService.import_from_files(
        db_session,
        CALCULATION / 'taxonomy_mapping.csv',
        'example_taxonomy')
    return taxonomy


@pytest.fixture(scope='module')
def example_vulnerability_structural(db_session):
    """Import structural vulnerability model."""
    return VulnerabilityService.import_from_files(
        db_session,
        CALCULATION / 'structural_vulnerability_model.xml',
        'example_structural')


@pytest.fixture(scope='module')
def example_vulnerability_nonstructural(db_session):
    """Import nonstructural vulnerability model."""
    return VulnerabilityService.import_from_files(
        db_session,
        CALCULATION / 'nonstructural_vulnerability_model.xml',
        'example_nonstructural')


@pytest.fixture(scope='module')
def example_vulnerability_contents(db_session):
    """Import contents vulnerability model."""
    return VulnerabilityService.import_from_files(
        db_session,
        CALCULATION / 'contents_vulnerability_model.xml',
        'example_contents')


@pytest.fixture(scope='module')
def example_vulnerability_downtime(db_session):
    """Import downtime vulnerability model."""
    return VulnerabilityService.import_from_files(
        db_session,
        CALCULATION / 'downtime_vulnerability_model.xml',
        'example_downtime')


@pytest.fixture(scope='module')
def example_vulnerability_occupants(db_session):
    """Import occupants vulnerability model."""
    return VulnerabilityService.import_from_files(
        db_session,
        CALCULATION / 'occupants_vulnerability_model.xml',
        'example_occupants')


@pytest.fixture(scope='module')
def example_loss_calculation(
        db_session,
        example_exposure,
        example_vulnerability_structural,
        example_vulnerability_nonstructural,
        example_vulnerability_contents,
        example_vulnerability_downtime,
        example_vulnerability_occupants,
        example_taxonomy):
    """Create and run example loss calculation."""
    # Configure loss calculation (scenario_risk)
    risk_file = configparser.ConfigParser()
    risk_file.read(str(CALCULATION / 'job.ini'))

    # Remove fragility section as it's not needed for loss calculation
    if risk_file.has_section('fragility'):
        risk_file.remove_section('fragility')

    risk_file['exposure']['exposure_file'] = str(example_exposure.oid)
    risk_file['vulnerability']['contents_vulnerability_file'] = str(
        example_vulnerability_contents.oid)
    risk_file['vulnerability']['business_interruption_vulnerability_file'] = \
        str(example_vulnerability_downtime.oid)
    risk_file['vulnerability']['structural_vulnerability_file'] = str(
        example_vulnerability_structural.oid)
    risk_file['vulnerability']['nonstructural_vulnerability_file'] = str(
        example_vulnerability_nonstructural.oid)
    risk_file['vulnerability']['occupants_vulnerability_file'] = str(
        example_vulnerability_occupants.oid)
    risk_file['vulnerability']['taxonomy_mapping_csv'] = str(
        example_taxonomy.oid)
    risk_file['hazard']['gmfs_csv'] = str(CALCULATION / 'gmfs.csv')
    risk_file['hazard']['sites_csv'] = str(CALCULATION / 'sites.csv')

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write loss calculation config
        loss_file_path = Path(tmpdirname) / 'loss_calculation.ini'
        with open(loss_file_path, 'w') as f:
            risk_file.write(f)

        # Import calculation config
        calculation_data = CalculationDataService.import_from_files(
            db_session, [loss_file_path], [1])

    # Run calculation
    calc_service = CalculationExecutionService(db_session)
    for data in calculation_data:
        calc, branch_settings = data

        if calc is None:
            continue

        calculation = calc_service.run_calculation(calc, branch_settings)
        if calculation.type == ECalculationType.LOSS:
            return calculation

    return None


@pytest.fixture(scope='module')
def example_damage_calculation(
        db_session,
        example_exposure,
        example_fragility,
        example_taxonomy):
    """Create and run example damage calculation."""
    # Configure damage calculation (scenario_damage)
    damage_file = configparser.ConfigParser()
    damage_file.read(str(CALCULATION / 'job.ini'))

    # Remove vulnerability and risk_calculation sections
    if damage_file.has_section('vulnerability'):
        damage_file.remove_section('vulnerability')
    if damage_file.has_section('risk_calculation'):
        damage_file.remove_section('risk_calculation')

    damage_file['general']['calculation_mode'] = 'scenario_damage'
    damage_file['exposure']['exposure_file'] = str(example_exposure.oid)
    damage_file['fragility']['structural_fragility_file'] = str(
        example_fragility.oid)
    damage_file['fragility']['taxonomy_mapping_csv'] = str(
        example_taxonomy.oid)
    damage_file['hazard']['gmfs_csv'] = str(CALCULATION / 'gmfs.csv')
    damage_file['hazard']['sites_csv'] = str(CALCULATION / 'sites.csv')

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write damage calculation config
        damage_file_path = Path(tmpdirname) / 'damage_calculation.ini'
        with open(damage_file_path, 'w') as f:
            damage_file.write(f)

        # Import calculation config
        calculation_data = CalculationDataService.import_from_files(
            db_session, [damage_file_path], [1])

    # Run calculation
    calc_service = CalculationExecutionService(db_session)
    for data in calculation_data:
        calc, branch_settings = data

        if calc is None:
            continue

        calculation = calc_service.run_calculation(calc, branch_settings)
        if calculation.type == ECalculationType.DAMAGE:
            return calculation

    return None


@pytest.fixture(scope='module')
def example_risk_assessment(
        db_session,
        example_loss_calculation,
        example_damage_calculation):
    """Create risk assessment from example calculations."""
    riskassessment = RiskAssessment(
        originid='smi:ch.ethz.sed/example',
        losscalculation_oid=example_loss_calculation.oid,
        damagecalculation_oid=example_damage_calculation.oid)

    populate_creation_info(riskassessment)
    return RiskAssessmentRepository.create(db_session, riskassessment)
