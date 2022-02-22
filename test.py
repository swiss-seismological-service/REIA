from core.database import session
from core.input import (  # noqa
    create_exposure_csv,
    create_exposure_xml,
    create_risk_ini,
    create_hazard_ini,
    create_vulnerability_xml)

from core.parsers import (
    parse_oq_exposure_file,
    parse_oq_vulnerability_file,
    parse_asset_csv,
    parse_oq_risk_file)

from core.crud import (
    create_asset_collection,
    create_loss_model,
    create_vulnerability_model)

from esloss.datamodel import LossModel

# PARSE INPUT
with open('model/exposure_assets_full.csv', 'r') as f:
    assets = parse_asset_csv(f)

with open('model/risk.ini', 'r') as r:
    risk_dict = parse_oq_risk_file(r)

with open('model/exposure.xml', 'r') as e:
    exposure = parse_oq_exposure_file(e)

with open('model/structural_vulnerability.xml', 'r') as s:
    model_struc, functions_struc = parse_oq_vulnerability_file(s)

with open('model/contents_vulnerability.xml', 'r') as s:
    model_cont, functions_cont = parse_oq_vulnerability_file(s)

# SAVE PARSED INPUT TO DATABASE
asset_collection_oid = create_asset_collection(exposure, assets)
vulnerability_struc_oid = create_vulnerability_model(
    model_struc, functions_struc)
vulnerability_cont_oid = create_vulnerability_model(model_cont, functions_cont)
loss_model_oid = create_loss_model(
    risk_dict,
    asset_collection_oid,
    [vulnerability_struc_oid,
     vulnerability_cont_oid])

# GET INPUT FROM DATABASE
losscategory = 'structural'
aggregateby = ''
lossmodel_oid = 1

loss_model = session.query(LossModel).get(lossmodel_oid)

vulnerability_model = next(
    v for v in loss_model.vulnerabilitymodels if v.losscategory == losscategory)

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

with open('test_output/exposure.xml', 'w') as f:
    f.write(exposure_xml.getvalue())

with open('test_output/exposure_assets.csv', 'w') as f:
    f.write(assets_csv.getvalue())

with open('test_output/vulnerability.xml', 'w') as f:
    f.write(vulnerability_xml.getvalue())

with open('test_output/hazard.ini', 'w') as f:
    f.write(hazard_ini.getvalue())

with open('test_output/risk.ini', 'w') as f:
    f.write(risk_ini.getvalue())


session.remove()
