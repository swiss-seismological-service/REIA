from core.utils import create_file_pointer

import io
import pandas as pd


def create_exposure_xml(
        exposure_model,
        template_name='core/templates/exposure.xml'):
    """ create an in memory exposure xml file for OpenQuake"""
    data = exposure_model._asdict()
    return create_file_pointer(template_name, data=data)


def create_vulnerability_xml(
        vulnerability_model,
        template_name='core/templates/vulnerability.xml'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = vulnerability_model._asdict()
    data['vulnerabilityfunctions'] = []
    for vulnerability_function in vulnerability_model.vulnerabilityfunctions:
        data['vulnerabilityfunctions'].append(vulnerability_function._asdict())
    return create_file_pointer(template_name, data=data)


def create_hazard_ini(
        loss_model,
        template_name='core/templates/prepare_risk.ini'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = loss_model._asdict()
    return create_file_pointer(template_name, data=data)


def create_risk_ini(loss_model, template_name='core/templates/risk.ini'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = loss_model._asdict()
    return create_file_pointer(template_name, data=data)


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
