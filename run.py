import time
from sqlalchemy import select
from core.db.crud import create_asset_collection, create_assets, create_vulnerability_model
from core.input import create_exposure_input, create_vulnerability_input
from core.parsers import parse_exposure, parse_vulnerability

from core.db import session
from esloss.datamodel.asset import Site
from esloss.datamodel.vulnerability import StructuralVulnerabilityModel
from core.utils import ini_to_dict
from settings import get_config


def main():

    config = get_config()

    with open(config.OQ_SETTINGS, 'r') as f:
        settings = ini_to_dict(f)  # noqa

    with open('model/exposure.xml', 'r') as e:
        exposure, assets = parse_exposure(e)

    # print(settings)
    # print(exposure)
    # print(assets)
    # asset_collection = create_asset_collection(exposure, session)
    # print(asset_collection._oid)
    # assets = create_assets(assets, asset_collection, session)
    # print(len(assets))
    # stmt = select(Site).where(
    #     Site._assetcollection_oid == asset_collection._oid)
    # sites = session.execute(stmt).scalars().all()
    # print(len(sites))

    # with open('model/structural_vulnerability.xml', 'r') as s:
    #     model_structural = parse_vulnerability(s)

    # print(model_structural)

    # vulnerability_struc = create_vulnerability_model(
    #     model_structural, session)

    # print(vulnerability_struc)

    # stmt = select(StructuralVulnerabilityModel.__table__).where(
    #     StructuralVulnerabilityModel._oid == vulnerability_struc._oid)

    # print(session.execute(stmt).unique().mappings().all())
    ############################################################

    # exposure.xml

    exposure_xml, exposure_csv = create_exposure_input(
        33, session)

    # # exposure_assets.csv
    # # vulnerability.xml
    # vulnerability_xml = create_vulnerability_input(
    #     vulnerability_struc._oid, session)
    # # pre-calculation.ini
    # hazard_ini = create_hazard_ini(loss_model)
    # # risk.ini
    # risk_ini = create_risk_ini(loss_model, 'site')

    with open('test_output/exposure.xml', 'w') as f:
        f.write(exposure_xml.getvalue())

    with open('test_output/exposure_assets.csv', 'w') as f:
        f.write(exposure_csv.getvalue())

    # with open('test_output/vulnerability.xml', 'w') as f:
    #     f.write(vulnerability_xml.getvalue())

    # with open('test_output/hazard.ini', 'w') as f:
    #     f.write(hazard_ini.getvalue())

    # with open('test_output/risk.ini', 'w') as f:
    #     f.write(risk_ini.getvalue())

    # response = oqapi_send_pre_calculation(hazard_ini,
    #                                       exposure_xml,
    #                                       assets_csv,
    #                                       vulnerability_xml)

    # if response.status_code >= 400:
    #     return None

    # # wait for pre-calculation to finish
    # pre_job_id = response.json()['job_id']
    # oqapi_wait_for_job(pre_job_id)
    # shakemap_zip = open('model/shapefiles.zip', 'rb')

    # response_main = oqapi_send_main_calculation(
    #     pre_job_id, risk_ini, shakemap_zip)
    # main_job_id = response_main.json()['job_id']
    # oqapi_wait_for_job(main_job_id)
    # loss_calculation = LossCalculation(
    #     shakemapid_resourceid='shakemap_address',
    #     _lossmodel_oid=loss_config._lossmodel_oid,
    #     losscategory=loss_config.losscategory,
    #     aggregateBy=loss_config.aggregateby,
    #     timestamp_starttime=datetime.now()
    # )
    # session.add(loss_calculation)
    # session.commit()
    # # wait, fetch and save results
    # fetch_oq_results.apply_async(
    #     [response_main.json()['job_id'], loss_calculation._oid])
#     from openquake.calculators.extract import WebExtractor
#     # fetch results
#     extractor = WebExtractor(4, 'http://localhost:8800')
#     arr, attrs = extractor.get('risk_by_event')
#     # json.loads(attrs['json'].tobytes())
#     print(attrs)
#     print(arr)


if __name__ == "__main__":
    main()
#     session.remove()
