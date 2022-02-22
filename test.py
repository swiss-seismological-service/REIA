from core.utils import (  # noqa
    create_exposure_csv,
    create_exposure_xml,
    create_risk_ini,
    create_hazard_ini,
    create_vulnerability_xml,
    ini_to_dict)

from core.parsers import (
    parse_oq_exposure_file,
    parse_oq_vulnerability_file,
    parse_asset_csv)

from core.crud import (
    create_asset_collection,
    create_loss_model,
    create_vulnerability_model)

from core.database import session

with open('model/exposure_assets_full.csv', 'r') as f:
    assets = parse_asset_csv(f)

with open('model/risk.ini', 'r') as r:
    risk_dict = ini_to_dict(r)

with open('model/exposure.xml', 'r') as e:
    exposure = parse_oq_exposure_file(e)

with open('model/structural_vulnerability.xml', 'r') as s:
    model, functions = parse_oq_vulnerability_file(s)

# asset_collection_oid = create_asset_collection(exposure, assets)
# vulnerability_model_oid = create_vulnerability_model(model, functions)
loss_model_oid = create_loss_model(risk_dict, 5, [1, 2])
print(loss_model_oid)

session.remove()
