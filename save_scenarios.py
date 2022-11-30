
import json
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler

from esloss.datamodel import EEarthquakeType

from core.actions import create_risk_scenario
from core.db import crud, session
from core.io import ERiskType
from core.io.read import combine_assets
from core.utils import aggregationtags_from_assets
from settings import get_config

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(filename)s.%(funcName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[TimedRotatingFileHandler('logs/datapipe.log',
                                       when='d',
                                       interval=1,
                                       backupCount=5),
              logging.StreamHandler()
              ]
)
LOGGER = logging.getLogger(__name__)


def run_scenario():
    start = time.perf_counter()
    data_folder = get_config().SCENARIO_DATA_FOLDER

    files = [
        f'{data_folder}/../../exposure/MIM/'
        'Exposure_MIM_RB_2km_v04.4_CH_mp3_allOcc_Aggbl.xml',
        f'{data_folder}/../../exposure/MIM/'
        'Exposure_MIM_RF_2km_v04.4_CH_mp3_allOcc_Aggbl.xml',
        f'{data_folder}/../../exposure/SAM/'
        'Exposure_SAM_RB_2km_v04.4_CH_mp5_allOcc_Aggbl.xml',
        f'{data_folder}/../../exposure/SAM/'
        'Exposure_SAM_RF_2km_v04.4_CH_mp5_allOcc_Aggbl.xml']

    assets = combine_assets(files)

    aggregation_tags = {}

    for type in ['Canton', 'CantonGemeinde']:
        existing_tags = crud.read_aggregationtags(type, session)
        all_tags, _ = aggregationtags_from_assets(assets, type, existing_tags)
        aggregation_tags.update({t.name: t for t in all_tags})

    session.add_all([v for v in aggregation_tags.values()])
    session.commit()

    with open('settings/scenarios/config.json') as f:
        scenario_configs = json.load(f)

    for config in scenario_configs:
        LOGGER.info(f'Starting to parse scenario {config["scenario_name"]}.')
        config['folder'] = f"{data_folder}/{config['folder']}"
        earthquake_oid = crud.create_or_update_earthquake_information(
            {'type': EEarthquakeType.SCENARIO, 'originid': config['originid']},
            session)

        LOGGER.info('Creating risk scenarios....')
        create_risk_scenario(earthquake_oid,
                             ERiskType.LOSS,
                             aggregation_tags,
                             config,
                             session)

        LOGGER.info('Creating damage scenarios....')
        create_risk_scenario(earthquake_oid,
                             ERiskType.DAMAGE,
                             aggregation_tags,
                             config,
                             session)

        LOGGER.info(f'Saving the scenario took {time.perf_counter()-start}.')
    session.remove()

    LOGGER.info(f'Saving all results took {time.perf_counter()-start}.')


if __name__ == "__main__":
    run_scenario()
