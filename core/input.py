import time
from typing import Tuple
from core.db.crud import (read_asset_collection,
                          read_vulnerability_model,
                          LOSSCATEGORY_OBJECT_MAPPING)
from core.parsers import ASSETS_COLS_MAPPING
from core.utils import create_file_pointer

from sqlalchemy.orm import Session
from esloss.datamodel.asset import Asset

import io
import pandas as pd


def create_vulnerability_input(
        vulnerability_model_oid: int,
        session: Session,
        template_name: str = 'core/templates/vulnerability.xml') -> io.StringIO:
    """
    create an in memory vulnerability xml file for OpenQuake
    """

    vulnerability_model = read_vulnerability_model(
        vulnerability_model_oid, session)

    data = vulnerability_model._asdict()
    data['_type'] = next((k for k, v in LOSSCATEGORY_OBJECT_MAPPING.items()
                          if v.__name__.lower() == data['_type']))
    data['vulnerabilityfunctions'] = []

    for vf in vulnerability_model.vulnerabilityfunctions:
        vf_dict = vf._asdict()
        vf_dict['lossratios'] = [lr._asdict() for lr in vf.lossratios]
        data['vulnerabilityfunctions'].append(vf_dict)

    return create_file_pointer(template_name, data=data)


def create_exposure_input(
    asset_collection_oid: int,
    session: Session,
    template_name='core/templates/exposure.xml') \
        -> Tuple[io.StringIO, io.StringIO]:
    """
    create an in memory exposure xml file for OpenQuake
    """

    asset_collection = read_asset_collection(asset_collection_oid, session)
    data = asset_collection._asdict()

    data['costtypes'] = [c._asdict() for c in asset_collection.costtypes]
    data['tagnames'] = {
        agg.type: agg.name for agg in asset_collection.aggregationtags}

    exposure_xml = create_file_pointer(template_name, data=data)

    exposure_df = assets_to_dataframe(asset_collection.assets)

    exposure_csv = io.StringIO()
    exposure_df.to_csv(exposure_csv)
    exposure_csv.seek(0)
    exposure_csv.name = 'exposure_assets.csv'

    return (exposure_xml, exposure_csv)


def assets_to_dataframe(assets: list[Asset]) -> pd.DataFrame:
    """
    create an in-memory assets csv file for OpenQuake
    """

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


def create_hazard_ini(
        loss_model,
        template_name='core/templates/prepare_risk.ini'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = loss_model._asdict()
    return create_file_pointer(template_name, data=data)


def create_risk_ini(
        loss_model,
        aggregate_by=None,
        template_name='core/templates/risk.ini'):
    """ create an in memory vulnerability xml file for OpenQuake"""
    data = loss_model._asdict()
    data['aggregateBy'] = aggregate_by
    return create_file_pointer(template_name, data=data)
