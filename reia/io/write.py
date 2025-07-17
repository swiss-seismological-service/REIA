import configparser
import io
import pickle
from pathlib import Path
from typing import Tuple

import pandas as pd
from sqlalchemy.orm import Session

from reia.datamodel.asset import Asset
from reia.db.crud import (read_asset_collection, read_fragility_model,
                          read_taxonomymap, read_vulnerability_model)
from reia.io import (ASSETS_COLS_MAPPING, LOSSCATEGORY_FRAGILITY_MAPPING,
                     LOSSCATEGORY_VULNERABILITY_MAPPING)
from reia.utils import create_file_pointer


def create_fragility_input(
    fragility_model_oid: int,
    session: Session,
    template_name: Path = Path('reia/templates/fragility.xml')) \
        -> io.StringIO:
    """Create an in memory fragility xml file for OpenQuake.

    Args:
        fragility_model_oid: oid of the VulnerabilityModel to be used.
        session: SQLAlchemy database session.
        template_name: Template to be used for the fragility file.

    Returns:
        Filepointer for exposure xml and one for csv list of assets.
    """

    fragility_model = read_fragility_model(
        fragility_model_oid, session)

    data = fragility_model._asdict()
    data['_type'] = next((k for k, v in
                          LOSSCATEGORY_FRAGILITY_MAPPING.items(
                          ) if k == data['_type'].value))
    data['fragilityfunctions'] = []

    for vf in fragility_model.fragilityfunctions:
        vf_dict = vf._asdict()
        vf_dict['limitstates'] = [lr._asdict() for lr in vf.limitstates]
        data['fragilityfunctions'].append(vf_dict)

    return create_file_pointer(template_name, data=data)


def create_taxonomymap_input(
    oid: int,
    session: Session,
    name: str = 'taxonomy_mapping.csv') \
        -> io.StringIO:
    """Create an in memory vulnerability xml file for OpenQuake.

    Args:
        oid: oid of the VulnerabilityModel to be used.
        session: SQLAlchemy database session.
        name: Template to be used for the vulnerability file.

    Returns:
        Filepointer for exposure xml and one for csv list of assets.
    """

    taxonomy_map = read_taxonomymap(oid, session)

    mappings = taxonomy_map.mappings
    mappings = pd.DataFrame([vars(s) for s in mappings], columns=[
        'taxonomy', 'conversion', 'weight'])

    taxonomy_map_csv = io.StringIO()
    mappings.to_csv(taxonomy_map_csv, index=False)
    taxonomy_map_csv.seek(0)
    taxonomy_map_csv.name = name

    return taxonomy_map_csv


def create_vulnerability_input(
    vulnerability_model_oid: int,
    session: Session,
    template_name: Path = Path('reia/templates/vulnerability.xml')) \
        -> io.StringIO:
    """Create an in memory vulnerability xml file for OpenQuake.

    Args:
        vulnerability_model_oid: oid of the VulnerabilityModel to be used.
        session: SQLAlchemy database session.
        template_name: Template to be used for the vulnerability file.

    Returns:
        Filepointer for exposure xml and one for csv list of assets.
    """

    vulnerability_model = read_vulnerability_model(
        vulnerability_model_oid, session)

    data = vulnerability_model._asdict()
    data['_type'] = next((k for k, v in
                          LOSSCATEGORY_VULNERABILITY_MAPPING.items(
                          ) if k == data['_type'].value))
    data['vulnerabilityfunctions'] = []

    for vf in vulnerability_model.vulnerabilityfunctions:
        vf_dict = vf._asdict()
        vf_dict['lossratios'] = [lr._asdict() for lr in vf.lossratios]
        data['vulnerabilityfunctions'].append(vf_dict)

    return create_file_pointer(template_name, data=data)


def create_exposure_input(
    asset_collection_oid: int,
    session: Session,
    template_name: Path = Path('reia/templates/exposure.xml'),
    assets_csv_name: Path = Path('exposure_assets.csv')) \
        -> Tuple[io.StringIO, io.StringIO]:
    """Creates in-memory exposure input files for OpenQuake.

    Args:
        asset_collection_oid: oid of the ExposureModel to be used.
        session: SQLAlchemy database session.
        template_name: Template to be used for the exposure file.
        assets_csv_name: Name for the assets CSV file.

    Returns:
        Filepointer for exposure xml and one for csv list of assets.
    """

    asset_collection = read_asset_collection(asset_collection_oid, session)
    data = asset_collection._asdict()

    data['assets_csv_name'] = assets_csv_name.name
    data['costtypes'] = [c._asdict() for c in asset_collection.costtypes]
    # first asset's tag types must be the same as all other's
    data['tagnames'] = [agg.type for agg in
                        asset_collection.assets[0].aggregationtags]

    exposure_xml = create_file_pointer(template_name, data=data)

    exposure_df = assets_to_dataframe(asset_collection.assets)

    exposure_csv = io.StringIO()
    exposure_df.to_csv(exposure_csv)
    exposure_csv.seek(0)
    exposure_csv.name = assets_csv_name.name

    return (exposure_xml, exposure_csv)


def assets_to_dataframe(assets: list[Asset]) -> pd.DataFrame:
    """Parses a list of Asset objects to a DataFrame."""

    assets_df = pd.DataFrame([x._asdict() for x in assets]).set_index('_oid')

    sites_df = pd.DataFrame([x.site._asdict() for x in assets])[
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

    job_file = create_job_file(working_job)
    job_file.name = 'job.ini'
    calculation_files.append(job_file)

    return calculation_files


def create_job_file(settings: configparser.ConfigParser) -> io.StringIO:
    job_ini = io.StringIO()
    settings.write(job_ini)
    job_ini.seek(0)

    return job_ini
