from ....celery.tasks import fetch_oq_results
from flask import make_response, request


from datetime import datetime

from . import api
from core.database import session
from esloss.datamodel import (VulnerabilityModel,
                              LossConfig, LossModel, LossCalculation)

from core.input import (
    create_exposure_csv,
    create_exposure_xml,
    create_risk_ini,
    create_hazard_ini,
    create_vulnerability_xml)
from core.oqapi import (
    oqapi_send_pre_calculation,
    oqapi_wait_for_job,
    oqapi_send_main_calculation)


@api.post('/calculation/run')
def post_calculation_run():
    # curl -X post http://localhost:5000/api/v1/calculation/run --header
    # "Content-Type: application/json" --data
    # '{"shakemap":"model/shapefiles.zip"}'

    # get data from database
    loss_config = session.query(LossConfig).get(1)
    loss_model = loss_config.lossmodel
    vulnerability_model = session.query(VulnerabilityModel) \
        .join(LossModel, VulnerabilityModel.lossmodels). \
        filter(VulnerabilityModel.losscategory == loss_config.losscategory)\
        .first()

    # exposure.xml
    exposure_xml = create_exposure_xml(loss_model.assetcollection)
    # exposure_assets.csv
    assets_csv = create_exposure_csv(loss_model.assetcollection.assets)
    # vulnerability.xml
    vulnerability_xml = create_vulnerability_xml(vulnerability_model)
    # pre-calculation.ini
    hazard_ini = create_hazard_ini(loss_model)
    # risk.ini
    risk_ini = create_risk_ini(loss_model)

    # parse request
    request_data = request.get_json()
    shakemap_zip = open(request_data['shakemap'], 'rb')
    # ...

    # send files to calculation endpoint
    response = oqapi_send_pre_calculation(hazard_ini,
                                          exposure_xml,
                                          assets_csv,
                                          vulnerability_xml)

    if response.status_code >= 400:
        return response

    # wait for pre-calculation to finish
    pre_job_id = response.json()['job_id']
    oqapi_wait_for_job(pre_job_id)

    # send main calculation
    response_main = oqapi_send_main_calculation(
        pre_job_id, risk_ini, shakemap_zip)

    if response_main.status_code >= 400:
        return response

    loss_calculation = LossCalculation(
        shakemapid_resourceid='shakemap_address',
        _lossmodel_oid=loss_config._lossmodel_oid,
        losscategory=loss_config.losscategory,
        aggregateBy=loss_config.aggregateby,
        timestamp_starttime=datetime.now()
    )
    session.add(loss_calculation)
    session.commit()

    # wait, fetch and save results
    fetch_oq_results.apply_async(
        [response_main.json()['job_id'], loss_calculation._oid])

    return make_response(response_main.json(), 200)
