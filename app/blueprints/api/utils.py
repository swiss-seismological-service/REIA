from logging import Logger
import time
import flask

from flask.helpers import make_response
from flask.json import jsonify
from werkzeug.exceptions import abort
from datamodel import Site

from flask import current_app

import configparser
import io
from typing import Tuple
import pandas as pd
import ast
import requests


def sites_from_assets(assets: pd.DataFrame) -> Tuple[list, list]:
    """
    Extract sites from assets dataframe

    :params assets: Dataframe of assets with 'lon' and 'lat' column
    :returns:       list of Site objects and list of group numbers for dataframe rows
    """
    # group by sites
    site_groups = assets.groupby(['lon', 'lat'])

    all_sites = []

    # create site models
    for name, _ in site_groups:
        site = Site(longitude_value=name[0],
                    latitude_value=name[1],
                    _assetcollection_oid=int(assets.iloc[0]['_assetcollection_oid']))
        all_sites.append(site)

    # return sites alongside with group index
    return all_sites, site_groups.grouper.group_info[0]


def ini_to_dict(filepointer: io.BytesIO) -> dict:
    # make sure ini has at least one section
    byte_str = filepointer.read()

    file_content = '[dummy_section]\n' + byte_str.decode('UTF-8')

    # read ini
    config = configparser.RawConfigParser()
    config.read_string(file_content)

    # parse to dict
    mydict = {}
    for k, v in {s: dict(config.items(s)) for s in config.sections()}.items():
        mydict.update({key: value for key, value in v.items()})

    # try and parse values to appropriate types
    for k, v in mydict.items():
        try:
            mydict[k] = ast.literal_eval(v)
        except:
            pass

    return mydict


def _create_file_pointer(template_name, **kwargs):
    """ create file pointer """
    sio = io.StringIO()
    template = current_app.jinja_env.get_template(template_name)
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.rsplit('/', 1)[-1]
    return sio


def create_exposure_xml(exposure_model, template_name='api/exposure.xml'):
    """ create an in memory exposure xml file for OpenQuake"""
    data = exposure_model._asdict()
    return _create_file_pointer(template_name, data=data)


def create_vulnerability_xml(vulnerability_model, template_name='api/vulnerability.xml'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = vulnerability_model._asdict()
    data['vulnerabilityfunctions'] = []
    for vulnerability_function in vulnerability_model.vulnerabilityfunctions:
        data['vulnerabilityfunctions'].append(vulnerability_function._asdict())
    return _create_file_pointer(template_name, data=data)


def create_hazard_ini(loss_model, template_name='api/prepare_risk.ini'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = loss_model._asdict()
    return _create_file_pointer(template_name, data=data)


def create_risk_ini(loss_model, template_name='api/risk.ini'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = loss_model._asdict()
    return _create_file_pointer(template_name, data=data)


def create_exposure_csv(assets):
    """ create an in-memory assets csv file for OpenQuake """
    assets_df = pd.DataFrame([x._asdict() for x in assets]).set_index('_oid')
    sites_df = pd.DataFrame([x.site._asdict() for x in assets])[
        ['longitude_value', 'latitude_value']]
    result_df = pd.concat(
        [assets_df, sites_df.set_index(assets_df.index)], axis=1)

    selector = {
        'longitude_value': 'lon',
        'latitude_value': 'lat',
        'taxonomy_concept': 'taxonomy',
        'buildingcount': 'number',
        'structuralvalue_value': 'structural',
        'contentvalue_value': 'contents',
        'occupancydaytime_value': 'day',
        '_postalcode_oid': 'postalcode',
        '_municipality_oid': 'municipality'}

    result_df = result_df.rename(columns=selector)[[*selector.values()]]
    result_df.index.name = 'id'
    exposure_assets_csv = io.StringIO()
    result_df.to_csv(exposure_assets_csv)
    exposure_assets_csv.seek(0)
    exposure_assets_csv.name = 'exposure_assets.csv'
    return exposure_assets_csv


def oqapi_send_pre_calculation(job_config, input_model_1, input_model_2, input_model_3):
    files = locals()

    try:
        response = requests.post(
            'http://localhost:8800/v1/calc/run', files=files)

        if response.ok:
            current_app.logger.info(
                'Successfully sent calculation job to OpenQuake.')
            return response
        else:
            current_app.logger.error(
                'Error sending the calculation job to OpenQuake.')
            return response
    except requests.exceptions.ConnectionError:
        current_app.logger.error('Could not connect to OpenQuake')
        return abort(make_response(jsonify({'error': 'Could not connect to the OpenQuake API'}), 400))


def oqapi_send_main_calculation(job_id, job_config, input_model_1):
    files = locals()

    try:
        response = requests.post(
            'http://localhost:8800/v1/calc/run', files=files,
            data={'hazard_job_id': job_id})

        if response.ok:
            current_app.logger.info(
                'Successfully sent calculation job to OpenQuake.')
            return response
        else:
            current_app.logger.error(
                'Error sending the calculation job to OpenQuake.')
            return response
    except requests.exceptions.ConnectionError:
        current_app.logger.error('Could not connect to OpenQuake')
        return abort(make_response(jsonify({'error': 'Could not connect to the OpenQuake API'}), 400))


def oqapi_get_job_status(job_id):
    try:
        return requests.get(f'http://localhost:8800/v1/calc/{job_id}/status')\
            .json()['status']
    except requests.exceptions.ConnectionError:
        current_app.logger.error('Could not connect to OpenQuake')
        return abort(make_response(jsonify({'error': 'Could not connect to the OpenQuake API'}), 400))


def oqapi_wait_for_job(job_id):
    check_status = oqapi_get_job_status(job_id)

    while check_status != 'complete':
        time.sleep(1)
        check_status = oqapi_get_job_status(job_id)
        if check_status == 'failed':
            return make_response(check_status.json(), 400)
