
import json
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler

from esloss.datamodel import EEarthquakeType

from core.db import crud, session
from core.db.scenario import create_damage_scenario, create_risk_scenario
from core.io.scenario import aggregationtags_from_files
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
        f'{data_folder}/exposure/MIM/'
        'Exposure_MIM_RB_2km_v04.4_CH_mp3_allOcc_Aggbl.xml',
        f'{data_folder}/exposure/MIM/'
        'Exposure_MIM_RF_2km_v04.4_CH_mp3_allOcc_Aggbl.xml',
        f'{data_folder}/exposure/SAM/'
        'Exposure_SAM_RB_2km_v04.4_CH_mp5_allOcc_Aggbl.xml',
        f'{data_folder}/exposure/SAM/'
        'Exposure_SAM_RF_2km_v04.4_CH_mp5_allOcc_Aggbl.xml']

    aggregation_types = ['Canton', 'CantonGemeinde']

    existing_tags = {agg: crud.read_aggregationtags(agg, session)
                     for agg in aggregation_types}

    aggregation_tags = aggregationtags_from_files(
        files, aggregation_types, existing_tags)

    session.add_all([v for v in aggregation_tags.values()])
    session.commit()

    with open('settings/scenarios.json') as f:
        scenario_configs = json.load(f)

    for config in scenario_configs:
        config['folder'] = f"{data_folder}/{config['folder']}"
        earthquake_oid = crud.create_or_update_earthquake_information(
            {'type': EEarthquakeType.SCENARIO, 'originid': config['originid']},
            session)

        LOGGER.info('Creating risk scenarios....')
        create_risk_scenario(earthquake_oid, aggregation_tags, config, session)

        LOGGER.info('Creating damage scenarios....')
        create_damage_scenario(
            earthquake_oid, aggregation_tags, config, session)

        LOGGER.info(f'Saving the scenario took {time.perf_counter()-start}')
    session.remove()

    LOGGER.info(f'Saving all results took {time.perf_counter()-start}')


if __name__ == "__main__":
    run_scenario()
