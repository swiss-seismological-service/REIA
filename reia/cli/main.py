import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import typer
from typer import Argument
from typing_extensions import Annotated

from reia.cli.extensions import plugin_manager
from reia.config.settings import get_settings
from reia.repositories import DatabaseSession
from reia.repositories.asset import (AggregationGeometryRepository,
                                     AssetRepository, ExposureModelRepository,
                                     SiteRepository)
from reia.repositories.calculation import (CalculationRepository,
                                           RiskAssessmentRepository)
from reia.repositories.fragility import (FragilityModelRepository,
                                         TaxonomyMapRepository)
from reia.repositories.vulnerability import VulnerabilityModelRepository
from reia.schemas.calculation_schemas import RiskAssessment
from reia.schemas.enums import ECalculationType
from reia.services.calculation import (CalculationDataService,
                                       run_test_calculation)
from reia.services.exposure import (ExposureService,
                                    add_geometries_from_shapefile)
from reia.services.fragility import FragilityService
from reia.services.gmfs import GMFService
from reia.services.logger import LoggerService
from reia.services.riskassessment import RiskAssessmentService
from reia.services.taxonomy import TaxonomyService
from reia.services.vulnerability import VulnerabilityService
from reia.utils import display_table

# Load settings and initialize logging once at startup
_settings = get_settings()
LoggerService.setup_logging(log_level=_settings.log_level)


def _get_alembic_directory():
    """Find the Alembic configuration directory."""

    # For installed package, alembic files are now in the reia/alembic package
    package_alembic = Path(__file__).parent.parent / "alembic" / "alembic.ini"
    current_dir_alembic = Path(os.getcwd()) / "alembic.ini"

    if package_alembic.exists():
        # Package is installed, use package location
        return package_alembic.parent
    elif current_dir_alembic.exists():
        # Development mode, use current directory
        return current_dir_alembic.parent
    else:
        typer.echo(
            "Cannot find alembic.ini. Make sure REIA is properly "
            "installed or run from project root.")
        raise typer.Exit(code=1)


app = typer.Typer(add_completion=False,
                  pretty_exceptions_enable=False)
db = typer.Typer()
exposure = typer.Typer()
vulnerability = typer.Typer()
fragility = typer.Typer()
taxonomymap = typer.Typer()
calculation = typer.Typer()
risk_assessment = typer.Typer()
gmfs = typer.Typer()

gmf_service = GMFService()


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v",
                                 help="Enable verbose logging")
) -> None:
    """REIA - Rapid Earthquake Impact Assessment Switzerland."""
    if verbose:
        # Re-initialize logging with DEBUG level
        LoggerService._initialized = False
        LoggerService.setup_logging(log_level='DEBUG')


app.add_typer(db, name='db',
              help='Database Commands')
app.add_typer(exposure, name='exposure',
              help='Manage Exposure Models')
app.add_typer(vulnerability, name='vulnerability',
              help='Manage Vulnerability Models')
app.add_typer(fragility, name='fragility',
              help='Manage Fragility Models')
app.add_typer(taxonomymap, name='taxonomymap',
              help='Manage Taxonomy Mappings')
app.add_typer(calculation, name='calculation',
              help='Create or execute calculations')
app.add_typer(risk_assessment, name='risk-assessment',
              help='Manage Risk Assessments')
app.add_typer(gmfs, name='gmfs',
              help='Manage GMF files')

# Load and register plugins
plugin_manager.register_plugins(app)


@db.command('migrate')
def run_alembic_upgrade() -> None:
    """Run Alembic migrations to upgrade database to latest version."""
    # Find Alembic configuration directory
    alembic_dir = _get_alembic_directory()

    # Run alembic with the -c flag to specify the config file location
    alembic_ini = alembic_dir / "alembic.ini"
    result = subprocess.run([sys.executable,
                             '-m',
                             'alembic',
                             '-c', str(alembic_ini),
                             'upgrade',
                             'head'],
                            capture_output=True,
                            text=True)
    if result.returncode == 0:
        typer.echo('Database migration completed successfully.')
        if result.stdout:
            typer.echo(result.stdout)
    else:
        typer.echo(f'Migration failed: {result.stderr}')
        raise typer.Exit(code=1)


@db.command('downgrade')
def run_alembic_downgrade(
    revision: Annotated[str, typer.Argument(
        help='Target revision to downgrade to (use "-- -1" '
        'for previous revision, base for empty DB)')] = "-1"
) -> None:
    """Run Alembic downgrade to a specific revision."""

    # Find Alembic configuration directory
    alembic_dir = _get_alembic_directory()
    alembic_ini = alembic_dir / "alembic.ini"

    typer.echo(f'Downgrading database to revision: {revision}')
    result = subprocess.run([sys.executable,
                             '-m',
                             'alembic',
                             '-c', str(alembic_ini),
                             'downgrade',
                             revision],
                            capture_output=True,
                            text=True)
    if result.returncode == 0:
        typer.echo(
            f'Database downgrade to {revision} completed successfully.')
        if result.stdout:
            typer.echo(result.stdout)
    else:
        typer.echo(f'Downgrade failed: {result.stderr}')
        raise typer.Exit(code=1)


@db.command('history')
def show_migration_history() -> None:
    """Show Alembic migration history."""

    # Find Alembic configuration directory
    alembic_dir = _get_alembic_directory()
    alembic_ini = alembic_dir / "alembic.ini"

    typer.echo('Fetching migration history...')
    result = subprocess.run([sys.executable,
                             '-m',
                             'alembic',
                             '-c', str(alembic_ini),
                             'history',
                             '--verbose'],
                            capture_output=True,
                            text=True)
    if result.returncode == 0:
        typer.echo('Migration History:')
        typer.echo(result.stdout)
    else:
        typer.echo(f'Failed to get history: {result.stderr}')
        raise typer.Exit(code=1)


@db.command('current')
def show_current_revision() -> None:
    """Show current Alembic revision."""
    # Find Alembic configuration directory
    alembic_dir = _get_alembic_directory()
    alembic_ini = alembic_dir / "alembic.ini"

    typer.echo('Fetching current database revision...')

    result = subprocess.run([sys.executable,
                             '-m',
                             'alembic',
                             '-c',
                             str(alembic_ini),
                             'current'],
                            capture_output=True,
                            text=True)
    if result.returncode == 0:
        typer.echo('Current Database Revision:')
        typer.echo(
            result.stdout if result.stdout else
            'No revision (empty database)')
    else:
        typer.echo(f'Failed to get current revision: {result.stderr}')
        raise typer.Exit(code=1)


@db.command('stamp')
def stamp_database(
    revision: Annotated[str, typer.Argument(
        help='Revision to stamp database with '
        '(use "head" for latest)')] = "head"
) -> None:
    """Stamp database with a specific revision without running migrations."""

    # Find Alembic configuration directory
    alembic_dir = _get_alembic_directory()
    alembic_ini = alembic_dir / "alembic.ini"

    typer.echo(f'Stamping database with revision: {revision}')
    result = subprocess.run([sys.executable,
                             '-m',
                             'alembic',
                             '-c',
                             str(alembic_ini),
                             'stamp',
                             revision],
                            capture_output=True,
                            text=True)
    if result.returncode == 0:
        typer.echo(f'Database stamped with revision: {revision}')
        if result.stdout:
            typer.echo(result.stdout)
    else:
        typer.echo(f'Stamp failed: {result.stderr}')
        raise typer.Exit(code=1)


@exposure.command('add')
def add_exposure(
    exposure: Annotated[Path, typer.Argument(
        help='Path to exposure model file')],
    name: Annotated[str, typer.Argument(
        help='Name for the exposure model')]
) -> int:
    """Add an exposure model from file."""
    with DatabaseSession() as session:
        exposuremodel = ExposureService.import_from_files(
            session, exposure, name)
        assets_count = AssetRepository.count_by_exposuremodel(
            session, exposuremodel.oid)
        sites_count = SiteRepository.count_by_exposuremodel(
            session, exposuremodel.oid)

    typer.echo(
        f'Successfully created exposure model with ID {exposuremodel.oid} '
        f'containing {assets_count} assets across {sites_count} sites.')

    return exposuremodel.oid


@exposure.command('delete')
def delete_exposure(
    exposuremodel_oid: Annotated[int, typer.Argument(
        help='ID of exposure model to delete')]
) -> None:
    """Delete an exposure model."""
    with DatabaseSession() as session:
        ExposureModelRepository.delete(session, exposuremodel_oid)
    typer.echo(
        f'Successfully deleted exposure model with ID {exposuremodel_oid}.')


@exposure.command('list')
def list_exposure() -> None:
    """List all exposure models."""
    with DatabaseSession() as session:
        exposuremodels = ExposureModelRepository.get_all(session)

    headers = ['ID', 'Name', 'Created']
    rows = [[ac.oid, ac.name, ac.creationinfo_creationtime]
            for ac in exposuremodels]

    display_table('List of existing exposure models:', headers, rows)


@exposure.command('create_file')
def create_exposure(
        id: Annotated[int, typer.Argument(
            help='ID of exposure model')],
        filename: Annotated[Path, typer.Argument(
            help='Base path for output files')]
) -> None:
    """Create input files for an exposure model."""
    with DatabaseSession() as session:
        xml_path, csv_path = ExposureService.export_to_file(
            session, id, str(filename))

    typer.echo(
        f'Successfully created exposure files: "{xml_path}" and "{csv_path}".')


@exposure.command('add_geometries')
def add_exposure_geometries(
        exposure_id: Annotated[int, typer.Argument(
            help='ID of the exposure model')],
        aggregationtype: Annotated[str, typer.Argument(
            help='Type of the aggregation')],
        tag_column_name: Annotated[str, typer.Argument(
            help='Name of the aggregation tag column')],
        filename: Annotated[Path, typer.Argument(
            help='Path to the shapefile')]
) -> None:
    """Add geometries to an exposure model from shapefile.

    The geometries are added to the exposure model and connected to the
    respective aggregation tag of the given aggregation type.
    Required columns in the shapefile are:
    - tag: the aggregation tag
    - name: the name of the geometry
    - geometry: the geometry
    """
    with DatabaseSession() as session:
        geometry_count = add_geometries_from_shapefile(
            session, exposure_id, filename, tag_column_name, aggregationtype)

    typer.echo(
        f'Successfully added {geometry_count} '
        f'geometries to exposure model {exposure_id} '
        f'for aggregation type "{aggregationtype}".')


@exposure.command('delete_geometries')
def delete_exposure_geometries(
        exposure_id: Annotated[int, typer.Argument(
            help='ID of exposure model')],
        aggregationtype: Annotated[str, typer.Argument(
            help='Aggregation type to delete')]
) -> None:
    """Delete geometries from an exposure model."""
    with DatabaseSession() as session:
        AggregationGeometryRepository.delete_by_exposuremodel(
            session, exposure_id, aggregationtype)
    typer.echo(
        'Successfully deleted geometries for '
        f'aggregation type "{aggregationtype}" '
        f'from exposure model {exposure_id}.')


@fragility.command('add')
def add_fragility(
        fragility: Annotated[Path, typer.Argument(
            help='Path to fragility model file')],
        name: Annotated[str, typer.Argument(
            help='Name for the fragility model')]
) -> int:
    """Add a fragility model from file."""
    with DatabaseSession() as session:
        fragility_model = FragilityService.import_from_files(
            session, fragility, name)

    typer.echo(
        f'Successfully created fragility model "{fragility_model.type}" '
        f'with ID {fragility_model.oid}.')
    return fragility_model.oid


@fragility.command('delete')
def delete_fragility(
    fragility_model_oid: Annotated[int, typer.Argument(
        help='ID of fragility model to delete')]
) -> None:
    """Delete a fragility model."""
    with DatabaseSession() as session:
        FragilityModelRepository.delete(session, fragility_model_oid)

    typer.echo(
        f'Successfully deleted fragility model with ID {fragility_model_oid}.')


@fragility.command('list')
def list_fragility() -> None:
    """List all fragility models."""
    with DatabaseSession() as session:
        fragility_models = FragilityModelRepository.get_all(session)

    headers = ['ID', 'Name', 'Type', 'Created']
    rows = [[vm.oid, vm.name, vm.type, vm.creationinfo_creationtime]
            for vm in fragility_models]

    display_table('List of existing fragility models:', headers, rows)


@fragility.command('create_file')
def create_fragility(
        id: Annotated[int, typer.Argument(help='ID of fragility model')],
        filename: Annotated[Path, typer.Argument(help='Output file path')]
) -> None:
    """Create input file for a fragility model."""
    with DatabaseSession() as session:
        created_filename = FragilityService.export_to_file(
            session, id, str(filename))

    typer.echo(f'Successfully created fragility file: "{created_filename}".')


@taxonomymap.command('add')
def add_taxonomymap(
        map_file: Annotated[Path, typer.Argument(
            help='Path to taxonomy mapping CSV file')],
        name: Annotated[str, typer.Argument(
            help='Name for the taxonomy mapping')]
) -> int:
    """Add a taxonomy mapping from file."""
    with DatabaseSession() as session:
        taxonomy_map = TaxonomyService.import_from_files(
            session, map_file, name)

    typer.echo(
        f'Successfully created taxonomy mapping with ID {taxonomy_map.oid}.')
    return taxonomy_map.oid


@taxonomymap.command('delete')
def delete_taxonomymap(
    taxonomymap_oid: Annotated[int, typer.Argument(
        help='ID of taxonomy mapping to delete')]
) -> None:
    """Delete a taxonomy mapping."""
    with DatabaseSession() as session:
        TaxonomyMapRepository.delete(session, taxonomymap_oid)
    typer.echo(
        f'Successfully deleted taxonomy mapping with ID {taxonomymap_oid}.')


@taxonomymap.command('list')
def list_taxonomymap() -> None:
    """List all taxonomy mappings."""
    with DatabaseSession() as session:
        taxonomy_maps = TaxonomyMapRepository.get_all(session)

    headers = ['ID', 'Name', 'Created']
    rows = [[tm.oid, tm.name, tm.creationinfo_creationtime]
            for tm in taxonomy_maps]

    display_table('List of existing taxonomy mappings:', headers, rows)


@taxonomymap.command('create_file')
def create_taxonomymap(
        id: Annotated[int, typer.Argument(help='ID of taxonomy mapping')],
        filename: Annotated[Path, typer.Argument(help='Output file path')]
) -> None:
    """Create input file for a taxonomy mapping."""
    with DatabaseSession() as session:
        created_filename = TaxonomyService.export_to_file(
            session, id, str(filename))

    typer.echo(
        f'Successfully created taxonomy mapping file: "{created_filename}".')


@vulnerability.command('add')
def add_vulnerability(
        vulnerability: Annotated[Path, typer.Argument(
            help='Path to vulnerability model file')],
        name: Annotated[str, typer.Argument(
            help='Name for the vulnerability model')]
) -> int:
    """Add a vulnerability model from file."""
    with DatabaseSession() as session:
        vulnerability_model = VulnerabilityService.import_from_files(
            session, vulnerability, name)

    typer.echo(
        'Successfully created vulnerability '
        f'model "{vulnerability_model.type}" '
        f'with ID {vulnerability_model.oid}.')
    return vulnerability_model.oid


@vulnerability.command('delete')
def delete_vulnerability(
    vulnerability_model_oid: Annotated[int, typer.Argument(
        help='ID of vulnerability model to delete')]
) -> None:
    """Delete a vulnerability model."""
    with DatabaseSession() as session:
        VulnerabilityModelRepository.delete(session, vulnerability_model_oid)
    typer.echo(
        'Successfully deleted vulnerability '
        f'model with ID {vulnerability_model_oid}.')


@vulnerability.command('list')
def list_vulnerability() -> None:
    """List all vulnerability models."""
    with DatabaseSession() as session:
        vulnerability_models = VulnerabilityModelRepository.get_all(session)

    headers = ['ID', 'Name', 'Type', 'Created']
    rows = [[vm.oid, vm.name, vm.type, vm.creationinfo_creationtime]
            for vm in vulnerability_models]

    display_table('List of existing vulnerability models:', headers, rows)


@vulnerability.command('create_file')
def create_vulnerability(
        id: Annotated[int, typer.Argument(help='ID of vulnerability model')],
        filename: Annotated[Path, typer.Argument(help='Output file path')]
) -> None:
    """Create input file for a vulnerability model."""
    with DatabaseSession() as session:
        created_filename = VulnerabilityService.export_to_file(
            session, id, str(filename))

    typer.echo(
        f'Successfully created vulnerability file: "{created_filename}".')


@calculation.command('create_files')
def create_calculation_files(
        target_folder: Annotated[Path, typer.Argument(
            help='Target folder for exported calculation files.')],
        settings_file: Annotated[Path, typer.Argument(
            help='Path to calculation settings file (eg. job.ini).')]
) -> None:
    """Create all input files to manually run a OpenQuake calculation."""
    with DatabaseSession() as session:
        CalculationDataService.export_branch_to_directory(
            session, settings_file, target_folder)

    typer.echo(
        'Successfully created OpenQuake calculation '
        f'files in folder "{str(target_folder)}".')


@calculation.command('run_test')
def run_test_calculation_cmd(
    settings_file: Annotated[Path, typer.Argument(
        help='Path to calculation settings file')]
) -> None:
    """Send a calculation to OpenQuake as a test, without storing results."""
    with DatabaseSession() as session:
        response = run_test_calculation(session, settings_file)

    typer.echo(
        f'Test calculation submitted successfully. Response: {response}')


@calculation.command('list')
def list_calculations(calc_type: Annotated[ECalculationType | None,
                                           typer.Option(
        help='Filter by earthquake type')] = None) -> None:
    """List all calculations, optionally filtered by calculation type."""

    with DatabaseSession() as session:
        calculations = CalculationRepository.get_all_by_type(
            session, type=calc_type)

    headers = ['ID', 'Status', 'Type', 'Created', 'Description']
    rows = [[c.oid, c.status.name, c.type.name,
             c.creationinfo_creationtime, c.description]
            for c in calculations]

    display_table('List of existing calculations:', headers, rows)


@calculation.command('delete')
def delete_calculation(
    calculation_oid: Annotated[int, typer.Argument(
        help='ID of calculation to delete')]
) -> None:
    """Delete a calculation."""
    with DatabaseSession() as session:
        CalculationRepository.delete(session, calculation_oid)
    typer.echo(
        f'Successfully deleted calculation with ID {calculation_oid}.')


@risk_assessment.command('add')
def add_risk_assessment(
        originid: Annotated[str, typer.Argument(
            help='Origin ID for the risk assessment')],
        loss_id: Annotated[int, typer.Argument(
            help='ID of loss calculation')],
        damage_id: Annotated[int, typer.Argument(
            help='ID of damage calculation')]
) -> int:
    """Add a risk assessment entry and assign a loss and damage calculation."""
    riskassessment = RiskAssessment(
        originid=originid,
        losscalculation_oid=loss_id,
        damagecalculation_oid=damage_id
    )
    with DatabaseSession() as session:
        added = RiskAssessmentRepository.create(session, riskassessment)

    typer.echo(
        f'Successfully created risk assessment for {added.originid} '
        f'with ID {added.oid}.')

    return added.oid


@risk_assessment.command('delete')
def delete_risk_assessment(
    riskassessment_oid: Annotated[str, typer.Argument(
        help='ID of risk assessment to delete')]
) -> None:
    """Delete a risk assessment and all associated calculations and results."""
    with DatabaseSession() as session:
        rowcount = RiskAssessmentRepository.delete(session, riskassessment_oid)
    typer.echo(f'Successfully deleted {rowcount} risk assessment(s).')


@risk_assessment.command('list')
def list_risk_assessment() -> None:
    """List all risk assessments."""
    with DatabaseSession() as session:
        risk_assessments = RiskAssessmentRepository.get_all(session)

    headers = ['ID', 'Status', 'Type', 'Created']
    rows = [[c.oid, c.status.name, c.type.name, c.creationinfo_creationtime]
            for c in risk_assessments]

    display_table('List of existing risk assessments:', headers, rows)


@risk_assessment.command('run')
def run_risk_assessment(
    config: Annotated[list[Path],
                      typer.Option("--config",
                                   "-c",
                                   help="Config file path (job.ini)")
                      ] = [],
    weight: Annotated[list[float],
                      typer.Option("--weight",
                                   "-w",
                                   help="Weight for the calculation branch.")
                      ] = [],
    originid: Annotated[str | None,
                        typer.Option("--originid",
                                     "-o",
                                     help="Origin ID for the risk assessment.")
                        ] = None,
    simple_config: Annotated[Path | None,
                             typer.Argument(help="Single config file.")
                             ] = None,
    time: Annotated[str | None,
                    typer.Option("--time",
                                 "-t",
                                 help="Origin time for the risk assessment.")
                    ] = None
) -> None:
    """
    Run a risk assessment with one or more calculation branches.

    You can provide either a single config file as argument (simple mode),
    containing all settings for the loss and damage calculations.

    Eg. `reia risk-assessment run simple_job.ini`
    \n\n
    Alternatively, you can provide separate config files for loss and damage
    calculations along with their weights (complex mode).

    Eg. `reia risk-assessment run --config loss_job.ini --config damage_job.ini
    --weight 1 --weight 1 --originid some_origin_id`
    \n\n
    You can provide multiple config files per loss and damge calculation with
    respective weights to run ensemble calculations.

    Eg. `reia risk-assessment run --config loss_job1.ini --config loss_job2.ini
    --config damage_job1.ini --config damage_job2.ini
    --weight 0.5 --weight 0.5 --weight 0.7 --weight 0.3
    --originid some_origin_id`
    \n\n
    Additionally, you can specify an origin time for the risk assessment
    using the `--time` option in ISO format (YYYY-MM-DDTHH:MM:SS). The
    appropriate day/night/transit values will be applied based on the
    provided time and exposure model. Configurable working hours are taken
    from the settings.
    """
    # Determine configs and weights
    if simple_config and not config:
        # Simple mode: single config file as argument
        configs = [simple_config]
        weights = [1.0]
    elif config and not simple_config:
        # Complex mode: multiple configs with weights
        configs = config
        weights = weight
        if weights is []:
            raise typer.BadParameter(
                "When using --config, must also provide --weight options")
    else:
        raise typer.BadParameter(
            "Must provide either a config file argument OR --config options")

    origin_time = datetime.fromisoformat(time) if time is not None else None

    # Display what will be executed
    typer.echo(f'Running risk assessment for: {originid}')
    typer.echo("Calculation branches:")
    for i, (cfg, wgt) in enumerate(zip(configs, weights), 1):
        typer.echo(f"  {i}. {cfg} (weight: {wgt})")

    with DatabaseSession() as session:
        service = RiskAssessmentService(session)
        risk_assessment = service.run_risk_assessment(
            originid, configs, weights, origin_time)

    typer.echo(
        f'Successfully completed risk assessment with status: '
        f'{risk_assessment.status.name}')

    return risk_assessment.oid


@gmfs.command('from-dstore')
def export_gmfs_from_dstore(dstore: Path, directory: Path):
    gmf_data, site_collection = gmf_service.export_from_datastore(str(dstore))

    with open(Path(directory, 'gmfs.csv'), 'w') as f:
        f.write(gmf_data.getvalue())

    with open(Path(directory, 'sites.csv'), 'w') as f:
        f.write(site_collection.getvalue())


@gmfs.command('sample')
def sample_gmfs(
    exposure_xml: Annotated[Path, Argument(help='Exposure xml file path.')],
    csv_files: Annotated[list[Path],
                         Argument(help='List of CSV shakemap file paths.')]
):
    try:
        gmf_service.sample_from_csv([exposure_xml], [str(f) for f in csv_files])
    except BaseException as e:
        typer.echo('Uncaught error sampling gmfs.')
        typer.echo(str(e))
        import errno
        sys.exit(errno.EINVAL)


@gmfs.command('shakemap')
def sample_shakemap(
    exposure_xml: Path,
    grid_xml: Path,
    uncertainty_xml: Path,
    imts: Annotated[list[str], Argument(
        help='List of intensity measure types (e.g., SA(0.3) SA(1.0))'
    )]
):
    gmf_service.sample_from_shakemap(
        [exposure_xml],
        str(grid_xml),
        str(uncertainty_xml),
        imts)
