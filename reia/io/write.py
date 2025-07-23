import configparser
import io
import pickle
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from reia.datamodel.asset import Asset
from reia.io import ASSETS_COLS_MAPPING
from reia.repositories.asset import ExposureModelRepository
from reia.repositories.fragility import (FragilityModelRepository,
                                         MappingRepository)
from reia.repositories.vulnerability import VulnerabilityModelRepository
from reia.utils import (create_file_pointer_configparser,
                        create_file_pointer_dataframe,
                        create_file_pointer_jinja)


def create_fragility_input(fragility_model_oid: int,
                           session: Session,
                           template_name: Path =
                           Path('reia/templates/fragility.xml')
                           ) -> io.StringIO:
    """Create an in memory fragility xml file for OpenQuake.

    Args:
        fragility_model_oid: oid of the FragilityModel to be used.
        session: SQLAlchemy database session.
        template_name: Template to be used for the fragility file.

    Returns:
        Filepointer for fragility input.
    """

    fragility_model = FragilityModelRepository.get_by_id(
        session, fragility_model_oid)

    data = fragility_model.model_dump(mode='json')

    return create_file_pointer_jinja(template_name, data=data)


def create_taxonomymap_input(oid: int,
                             session: Session,
                             name: str = 'taxonomy_mapping.csv'
                             ) -> io.StringIO:
    """Create an in memory taxonomy mapping CSV file for OpenQuake.

    Args:
        oid: oid of the TaxonomyMap to be used.
        session: SQLAlchemy database session.
        name: Name of the file to be created.

    Returns:
        Filepointer for taxonomy mapping CSV.
    """

    mappings = MappingRepository.get_by_taxonomymap_oid(session, oid)
    mappings = mappings[['taxonomy', 'conversion', 'weight']]
    return create_file_pointer_dataframe(mappings, name=name, index=False)


def create_vulnerability_input(vulnerability_model_oid: int,
                               session: Session,
                               template_name: Path =
                               Path('reia/templates/vulnerability.xml')
                               ) -> io.StringIO:
    """Create an in memory vulnerability xml file for OpenQuake.

    Args:
        vulnerability_model_oid: oid of the VulnerabilityModel to be used.
        session: SQLAlchemy database session.
        template_name: Template to be used for the vulnerability file.

    Returns:
        Filepointer for vulnerability XML.
    """
    vulnerability_model = VulnerabilityModelRepository.get_by_id(
        session, vulnerability_model_oid)

    data = vulnerability_model.model_dump(mode='json')

    return create_file_pointer_jinja(template_name, data=data)


def create_exposure_input(asset_collection_oid: int,
                          session: Session,
                          template_name: Path =
                          Path('reia/templates/exposure.xml'),
                          assets_csv_name: Path =
                          Path('exposure_assets.csv')
                          ) -> tuple[io.StringIO, io.StringIO]:
    """Creates in-memory exposure input files for OpenQuake.

    Args:
        asset_collection_oid: oid of the ExposureModel to be used.
        session: SQLAlchemy database session.
        template_name: Template to be used for the exposure file.
        assets_csv_name: Name for the assets CSV file.

    Returns:
        Filepointer for exposure xml and one for csv list of assets.
    """

    exposuremodel = ExposureModelRepository.get_by_id(session,
                                                      asset_collection_oid)

    data = exposuremodel.model_dump(mode='json')

    data['assets_csv_name'] = assets_csv_name.name

    exposure_xml = create_file_pointer_jinja(template_name, data=data)

    exposure_df = assets_to_dataframe(exposuremodel.assets)

    exposure_csv = create_file_pointer_dataframe(
        exposure_df, name=assets_csv_name.name)

    return (exposure_xml, exposure_csv)


def assets_to_dataframe(assets: list[Asset]) -> pd.DataFrame:
    """Convert a list of Asset objects to a pandas DataFrame.
    
    Combines asset data with associated site coordinates and aggregation tags
    into a single DataFrame formatted for OpenQuake exposure input.
    
    Args:
        assets: List of Asset objects to convert.
        
    Returns:
        DataFrame with asset data, coordinates, and aggregation tags.
    """

    assets_df = pd.DataFrame([x.model_dump(mode='json')
                             for x in assets]).set_index('_oid')

    sites_df = pd.DataFrame([x.site.model_dump(mode='json') for x in assets])[
        ['longitude', 'latitude']]

    aggregationtags_df = pd.DataFrame(map(
        lambda asset: {tag.type: tag.name for tag in asset.aggregationtags},
        assets))

    result_df = pd.concat([assets_df,
                           sites_df.set_index(assets_df.index),
                           aggregationtags_df.set_index(assets_df.index)],
                          axis=1)

    selector = {**{'longitude': 'lon', 'latitude': 'lat'},
                **{v: k for k, v in ASSETS_COLS_MAPPING.items()},
                **{k: k for k in aggregationtags_df.columns}}

    result_df = result_df.rename(columns=selector)[[*selector.values()]] \
        .dropna(axis=1, how='all') \
        .fillna(0)
    result_df.index.name = 'id'

    return result_df


def assemble_calculation_input(job: configparser.ConfigParser,
                               session: Session) -> list[io.StringIO]:
    """Assemble all input files needed for an OpenQuake calculation.
    
    Creates in-memory file objects for all calculation inputs including
    exposure, vulnerability/fragility, taxonomy mapping, hazard files,
    and the job configuration file.
    
    Args:
        job: ConfigParser object containing calculation configuration.
        session: SQLAlchemy database session.
        
    Returns:
        List of StringIO file objects for the calculation.
    """
    # create deep copy of configparser
    tmp = pickle.dumps(job)
    working_job = pickle.loads(tmp)

    calculation_files = []

    exposure_xml, exposure_csv = create_exposure_input(
        working_job['exposure']['exposure_file'], session)
    exposure_xml.name = 'exposure.xml'
    working_job['exposure']['exposure_file'] = exposure_xml.name

    calculation_files.extend([exposure_xml, exposure_csv])

    if 'vulnerability' in working_job.keys():
        for k, v in working_job['vulnerability'].items():
            if k == 'taxonomy_mapping_csv':
                file = create_taxonomymap_input(v, session)
                file.name = "{}.csv".format(k.replace('_file', ''))
            else:
                file = create_vulnerability_input(v, session)
                file.name = "{}.xml".format(k.replace('_file', ''))
            working_job['vulnerability'][k] = file.name
            calculation_files.append(file)

    elif 'fragility' in working_job.keys():
        for k, v in working_job['fragility'].items():
            if k == 'taxonomy_mapping_csv':
                file = create_taxonomymap_input(v, session)
            else:
                file = create_fragility_input(v, session)
                file.name = "{}.xml".format(k.replace('_file', ''))
            working_job['fragility'][k] = file.name
            calculation_files.append(file)

    for k, v in working_job['hazard'].items():
        with open(v, 'r') as f:
            file = io.StringIO(f.read())
        file.name = Path(v).name
        working_job['hazard'][k] = file.name
        calculation_files.append(file)

    job_file = create_file_pointer_configparser(working_job, 'job.ini')

    calculation_files.append(job_file)

    return calculation_files
