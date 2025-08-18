import configparser
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from reia.repositories.calculation import RiskAssessmentRepository
from reia.repositories.tests.database import (get_test_session,
                                              setup_test_database,
                                              teardown_test_database)
from reia.schemas.calculation_schemas import RiskAssessment
from reia.services.calculation import (CalculationDataService,
                                       CalculationService)
from reia.services.exposure import ExposureService
from reia.services.fragility import FragilityService
from reia.services.taxonomy import TaxonomyService
from reia.services.vulnerability import VulnerabilityService
from reia.webservice.tests.database import (cleanup_test_client,
                                            get_test_async_session,
                                            get_test_client)

DATAFOLDER = Path(__file__).parent / 'data' / 'ria_test'


@pytest.fixture(scope='session', autouse=True)
def setup_test_env():
    """Set up test environment with database."""
    setup_test_database()
    yield
    teardown_test_database()


@pytest.fixture(scope='module')
def db_session():
    """Database session fixture that matches production setup."""
    session = get_test_session()
    yield session
    session.remove()


@pytest_asyncio.fixture
async def async_db_session():
    """Async database session for webservice tests."""

    async_session = get_test_async_session()
    engine = async_session.bind

    async with async_session() as session:
        yield session
        await session.close()

    await engine.dispose()


@pytest_asyncio.fixture
async def test_client():
    """Test client for FastAPI webservice."""

    client, sessionmanager = await get_test_client()

    async with client:
        yield client

    # Cleanup
    await cleanup_test_client(sessionmanager)


@pytest.fixture(scope='module')
def exposure(db_session):
    exposure = ExposureService.import_from_file(
        db_session, DATAFOLDER / 'exposure_test.xml', 'test')
    return exposure


@pytest.fixture(scope='module')
def fragility(db_session):
    fragility = FragilityService.import_from_file(
        db_session, DATAFOLDER / 'fragility_test.xml', 'test')
    return fragility


@pytest.fixture(scope='module')
def taxonomy(db_session):
    taxonomy = TaxonomyService.import_from_file(
        db_session, DATAFOLDER / 'taxonomy_test.csv', 'test')
    return taxonomy


@pytest.fixture(scope='module')
def vulnerability(db_session):
    vulnerability = VulnerabilityService.import_from_file(
        db_session, DATAFOLDER / 'vulnerability_test.xml', 'test')
    return vulnerability


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

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to temporary file
        loss_file_path = Path(tmpdirname) / 'loss_calculation.ini'
        with open(loss_file_path, 'w') as f:
            risk_file.write(f)

        yield loss_file_path


@pytest.fixture(scope='module')
def loss_calculation(loss_config, db_session):

    calculation, branch_settings = CalculationDataService.import_from_file(
        db_session, [loss_config], [1])

    calc_service = CalculationService(db_session)
    return calc_service.run_calculations(calculation, branch_settings)


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

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to temporary file
        damage_file_path = Path(tmpdirname) / 'damage_calculation.ini'
        with open(damage_file_path, 'w') as f:
            damage_file.write(f)

        yield damage_file_path


@pytest.fixture(scope='module')
def damage_calculation(damage_config, db_session):

    calculation, branch_settings = CalculationDataService.import_from_file(
        db_session, [damage_config], [1])

    calc_service = CalculationService(db_session)
    return calc_service.run_calculations(calculation, branch_settings)


@pytest.fixture(scope='module')
def risk_assessment(loss_calculation, damage_calculation, db_session):
    riskassessment = RiskAssessment(
        originid='smi:ch.ethz.sed/test',
        losscalculation_oid=loss_calculation.oid,
        damagecalculation_oid=damage_calculation.oid)
    return RiskAssessmentRepository.create(db_session, riskassessment)
