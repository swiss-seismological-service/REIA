import configparser
import traceback
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import shapely
import typer
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from typing_extensions import Annotated

from reia.actions import (dispatch_openquake_calculation,
                          run_openquake_calculations)
from reia.datamodel import EEarthquakeType, EStatus
from reia.db import crud, drop_db, init_db, init_db_file, session
from reia.io import CalculationBranchSettings
from reia.io.read import (parse_exposure, parse_fragility, parse_taxonomy_map,
                          parse_vulnerability)
from reia.io.write import (assemble_calculation_input, create_exposure_input,
                           create_fragility_input, create_taxonomymap_input,
                           create_vulnerability_input)

app = typer.Typer(add_completion=False)
db = typer.Typer()
exposure = typer.Typer()
vulnerability = typer.Typer()
fragility = typer.Typer()
taxonomymap = typer.Typer()
calculation = typer.Typer()
risk_assessment = typer.Typer()

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


@db.command('drop')
def drop_database():
    '''
    Drop all tables.
    '''
    drop_db()
    typer.echo('Tables dropped.')


@db.command('init')
def initialize_database():
    '''
    Create all tables.
    '''
    init_db()
    typer.echo('Tables created.')


@db.command('createall')
def create_all_to_file():
    init_db_file()


@exposure.command('add')
def add_exposure(exposure: Path, name: str):
    '''
    Add an exposure model.
    '''
    with open(exposure, 'r') as f:
        exposure, assets = parse_exposure(f)

    exposure['name'] = name

    asset_collection = crud.create_asset_collection(exposure, session)

    asset_objects = crud.create_assets(assets, asset_collection._oid, session)
    sites = crud.read_sites(asset_collection._oid, session)

    typer.echo(f'Created asset collection with ID {asset_collection._oid} and '
               f'{len(sites)} sites with {len(asset_objects)} assets.')
    session.remove()
    return asset_collection._oid


@exposure.command('delete')
def delete_exposure(asset_collection_oid: int):
    '''
    Delete an exposure model.
    '''
    crud.delete_asset_collection(asset_collection_oid, session)
    typer.echo(
        f'Deleted exposure model with ID {asset_collection_oid}.')
    session.remove()


@exposure.command('list')
def list_exposure():
    '''
    List all exposure models.
    '''
    asset_collections = crud.read_asset_collections(session)

    typer.echo('List of existing asset collections:')
    typer.echo('{0:<10} {1:<25} {2}'.format(
        'ID',
        'Name',
        'Creationtime'))

    for ac in asset_collections:
        typer.echo('{0:<10} {1:<25} {2}'.format(
            ac._oid,
            ac.name or '',
            str(ac.creationinfo_creationtime)))
    session.remove()


@exposure.command('create_file')
def create_exposure(id: int, filename: Path):
    '''
    Create input files for an exposure model.
    '''
    p_xml = filename.with_suffix('.xml')
    p_csv = filename.with_suffix('.csv')
    fp_xml, fp_csv = create_exposure_input(id, session, assets_csv_name=p_csv)
    session.remove()

    p_xml.parent.mkdir(exist_ok=True)
    p_xml.open('w').write(fp_xml.getvalue())
    p_csv.open('w').write(fp_csv.getvalue())

    if p_xml.exists() and p_csv.exists():
        typer.echo(
            f'Successfully created file "{str(p_xml)}" and "{str(p_csv)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@exposure.command('create_geometries')
def add_exposure_geometries(
        exposure_id:
        Annotated[int, typer.Argument(help='ID of the exposure model')],
        aggregationtype:
        Annotated[str, typer.Argument(help='type of the aggregation')],
        tag_column_name:
        Annotated[str, typer.Argument(
            help='name of the aggregation tag column')],
        filename:
        Annotated[Path, typer.Argument(help='path to the shapefile')]):
    '''
    Add geometries to an exposure model.

    The geometries are added to the exposuremodel and connected to the
    respective aggregationtag of the given aggregationtype.
    Required columns in the shapefile are:\n
    - tag: the aggregationtag\n
    - name: the name of the geometry\n
    - geometry: the geometry
    '''
    gdf = pd.DataFrame(gpd.read_file(filename))

    gdf['geometry'] = gdf['geometry'].apply(
        lambda x: MultiPolygon([x]) if isinstance(x, Polygon) else x)
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.force_2d(x).wkt)

    gdf = gdf[[tag_column_name, 'geometry', 'name']]
    gdf = gdf.rename(columns={tag_column_name: 'aggregationtag'})
    gdf['_aggregationtype'] = aggregationtype

    crud.create_geometries(exposure_id, gdf, session)

    session.remove()


@exposure.command('delete_geometries')
def delete_exposure_geometries(exposure_id: int, aggregationtype: str):
    crud.delete_geometries(exposure_id, aggregationtype, session)
    session.remove()


@fragility.command('add')
def add_fragility(fragility: Path, name: str):
    '''
    Add a fragility model.
    '''

    with open(fragility, 'r') as f:
        model = parse_fragility(f)

    model['name'] = name

    fragility_model = crud.create_fragility_model(model, session)
    typer.echo(
        f'Created fragility model of type "{fragility_model._type}"'
        f' with ID {fragility_model._oid}.')
    session.remove()
    return fragility_model._oid


@fragility.command('delete')
def delete_fragility(fragility_model_oid: int):
    '''
    Delete a fragility model.
    '''
    crud.delete_fragility_model(fragility_model_oid, session)
    typer.echo(f'Deleted fragility model with ID {fragility_model_oid}.')
    session.remove()


@fragility.command('list')
def list_fragility():
    '''
    List all fragility models.
    '''
    fragility_models = crud.read_fragility_models(session)

    typer.echo('List of existing fragility models:')
    typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
        'ID',
        'Name',
        'Type',
        'Creationtime'))

    for vm in fragility_models:
        typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
            vm._oid,
            vm.name or "",
            vm._type,
            str(vm.creationinfo_creationtime)))
    session.remove()


@fragility.command('create_file')
def create_fragility(id: int, filename: Path):
    '''
    Create input file for a fragility model.
    '''
    filename = filename.with_suffix('.xml')
    file_pointer = create_fragility_input(id, session)
    session.remove()

    filename.parent.mkdir(exist_ok=True)
    filename.open('w').write(file_pointer.getvalue())

    if filename.exists():
        typer.echo(
            f'Successfully created file "{str(filename)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@taxonomymap.command('add')
def add_taxonomymap(map_file: Path, name: str):
    '''
    Add a taxonomy mapping model.
    '''
    with open(map_file, 'r') as f:
        mapping = parse_taxonomy_map(f)

    taxonomy_map = crud.create_taxonomy_map(mapping, name, session)
    typer.echo(
        f'Created taxonomy map with ID {taxonomy_map._oid}.')
    session.remove()
    return taxonomy_map._oid


@taxonomymap.command('delete')
def delete_taxonomymap(taxonomymap_oid: int):
    '''
    Delete a vulnerability model.
    '''
    crud.delete_taxonomymap(taxonomymap_oid, session)
    typer.echo(
        f'Deleted taxonomy map with ID {taxonomymap_oid}.')
    session.remove()


@taxonomymap.command('list')
def list_taxonomymap():
    '''
    List all vulnerability models.
    '''
    taxonomy_maps = crud.read_taxonomymaps(session)

    typer.echo('List of existing vulnerability models:')
    typer.echo('{0:<10} {1:<25} {2}'.format(
        'ID',
        'Name',
        'Creationtime'))

    for tm in taxonomy_maps:
        typer.echo('{0:<10} {1:<25} {2}'.format(
            tm._oid,
            tm.name or "",
            str(tm.creationinfo_creationtime)))
    session.remove()


@taxonomymap.command('create_file')
def create_taxonomymap(id: int, filename: Path):
    '''
    Create input file for a taxonomy mapping.
    '''
    filename = filename.with_suffix('.csv')
    file_pointer = create_taxonomymap_input(id, session)
    session.remove()

    filename.parent.mkdir(exist_ok=True)
    filename.open('w').write(file_pointer.getvalue())

    if filename.exists():
        typer.echo(
            f'Successfully created file "{str(filename)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@vulnerability.command('add')
def add_vulnerability(vulnerability: Path, name: str):
    '''
    Add a vulnerability model.
    '''
    with open(vulnerability, 'r') as f:
        model = parse_vulnerability(f)
    model['name'] = name

    vulnerability_model = crud.create_vulnerability_model(model, session)

    typer.echo(
        f'Created vulnerability model of type "{vulnerability_model._type}"'
        f' with ID {vulnerability_model._oid}.')
    session.remove()
    return vulnerability_model._oid


@vulnerability.command('delete')
def delete_vulnerability(vulnerability_model_oid: int):
    '''
    Delete a vulnerability model.
    '''
    crud.delete_vulnerability_model(vulnerability_model_oid, session)
    typer.echo(
        f'Deleted vulnerability model with ID {vulnerability_model_oid}.')
    session.remove()


@vulnerability.command('list')
def list_vulnerability():
    '''
    List all vulnerability models.
    '''
    vulnerability_models = crud.read_vulnerability_models(session)

    typer.echo('List of existing vulnerability models:')
    typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
        'ID',
        'Name',
        'Type',
        'Creationtime'))

    for vm in vulnerability_models:
        typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
            vm._oid,
            vm.name or "",
            vm._type,
            str(vm.creationinfo_creationtime)))
    session.remove()


@vulnerability.command('create_file')
def create_vulnerability(id: int, filename: Path):
    '''
    Create input file for a vulnerability model.
    '''
    filename = filename.with_suffix('.xml')
    file_pointer = create_vulnerability_input(id, session)
    session.remove()

    filename.parent.mkdir(exist_ok=True)
    filename.open('w').write(file_pointer.getvalue())

    if filename.exists():
        typer.echo(
            f'Successfully created file "{str(filename)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@calculation.command('create_files')
def create_calculation_files(target_folder: Path,
                             settings_file: Path):
    '''
    Create all files for an OpenQuake calculation.
    '''
    target_folder.mkdir(exist_ok=True)

    job_file = configparser.ConfigParser()
    job_file.read(settings_file)

    files = assemble_calculation_input(job_file, session)

    for file in files:
        with open(target_folder / file.name, 'w') as f:
            f.write(file.getvalue())

    typer.echo('Openquake calculation files created '
               f'in folder "{str(target_folder)}".')

    session.remove()


@calculation.command('run_test')
def run_test_calculation(settings_file: Path):
    '''
    Send a calculation to OpenQuake as a test.
    '''
    job_file = configparser.ConfigParser()
    job_file.read(settings_file)

    response = dispatch_openquake_calculation(job_file, session)

    typer.echo(response.json())

    session.remove()


@calculation.command('run')
def run_calculation(
        settings: list[str] = typer.Option(...),
        weights: list[float] = typer.Option(...)):
    '''
    Run an OpenQuake calculation.
    '''
    # console input validation
    if settings and not len(settings) == len(weights):
        typer.echo('Error: Number of setting files and weights provided '
                   'have to be equal. Exiting...')
        raise typer.Exit(code=1)

    # input parsing
    settings = zip(weights, settings)

    branch_settings = []
    for s in settings:
        job_file = configparser.ConfigParser()
        job_file.read(Path(s[1]))
        branch_settings.append(CalculationBranchSettings(s[0], job_file))

    run_openquake_calculations(branch_settings, session)

    session.remove()


@calculation.command('list')
def list_calculations(eqtype: Optional[EEarthquakeType] = typer.Option(None)):
    '''
    List all calculations.
    '''
    calculations = crud.read_calculations(session, eqtype)

    typer.echo('List of existing calculations:')
    typer.echo('{0:<10} {1:<25} {2:<25} {3:<30} {4}'.format(
        'ID',
        'Status',
        'Type',
        'Created',
        'Description'))

    for c in calculations:
        typer.echo('{0:<10} {1:<25} {2:<25} {3:<30} {4}'.format(
            c._oid,
            c.status.name,
            c._type.name,
            str(c.creationinfo_creationtime),
            c.description))
    session.remove()


@calculation.command('delete')
def delete_calculation(calculation_oid: int):
    '''
    Delete a calculation.
    '''
    crud.delete_calculation(calculation_oid, session)
    typer.echo(
        f'Deleted calculation with ID {calculation_oid}.')
    session.remove()


@risk_assessment.command('add')
def add_risk_assessment(originid: str, loss_id: int, damage_id: int):
    '''
    Add a risk assessment.
    '''
    added = crud.create_risk_assessment(
        originid,
        session,
        _losscalculation_oid=loss_id,
        _damagecalculation_oid=damage_id)

    typer.echo(
        f'added risk_assessment for {added.originid} with '
        f'ID {added._oid}.')

    session.remove()

    return added._oid


@risk_assessment.command('delete')
def delete_risk_assessment(risk_assessment_oid: int):
    '''
    Delete a risk assessment.
    '''
    rowcount = crud.delete_risk_assessment(risk_assessment_oid, session)
    typer.echo(f'Deleted {rowcount} risk assessment.')
    session.remove()


@risk_assessment.command('list')
def list_risk_assessment():
    '''
    List risk assessments.
    '''
    risk_assessments = crud.read_risk_assessments(session)

    typer.echo('List of existing risk assessments:')
    typer.echo('{0:<10} {1:<25} {2:<25} {3}'.format(
        'ID',
        'Status',
        'Type',
        'Created'))

    for c in risk_assessments:
        typer.echo('{0:<10} {1:<25} {2:<25} {3}'.format(
            c._oid,
            c.status.name,
            c.type.name,
            str(c.creationinfo_creationtime)))
    session.remove()


@risk_assessment.command('run')
def run_risk_assessment(
        originid: str,
        loss: str = typer.Option(...),
        damage: str = typer.Option(...)):
    '''
    Run a risk assessment.
    '''
    typer.echo('Running risk assessment:')
    typer.echo('Starting loss calculations...')

    risk_assessment = crud.create_risk_assessment(
        originid, session, type=EEarthquakeType.NATURAL,
        status=EStatus.CREATED)
    try:
        job_file_loss = configparser.ConfigParser()
        job_file_loss.read(Path(loss))
        loss_branch = CalculationBranchSettings(1, job_file_loss)
        risk_assessment = crud.update_risk_assessment_status(
            risk_assessment._oid, EStatus.EXECUTING, session)
        losscalculation = run_openquake_calculations([loss_branch], session)

        risk_assessment._losscalculation_oid = losscalculation._oid

        typer.echo('Starting damage calculations...')
        job_file_damage = configparser.ConfigParser()
        job_file_damage.read(Path(damage))
        damage_branch = CalculationBranchSettings(1, job_file_damage)
        damagecalculation = run_openquake_calculations(
            [damage_branch], session)

        risk_assessment._damagecalculation_oid = damagecalculation._oid
        risk_assessment.status = EStatus(
            min(losscalculation.status.value, damagecalculation.status.value))
    except BaseException as e:
        risk_assessment.status = EStatus.ABORTED if isinstance(
            e, KeyboardInterrupt) else EStatus.FAILED
        traceback.print_exc()
    finally:
        session.commit()
        session.remove()
        typer.echo('Done.')
