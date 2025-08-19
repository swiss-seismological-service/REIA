from pathlib import Path

import numpy as np
import pandas as pd

from reia.services.exposure import ExposureService
from reia.services.fragility import FragilityService

DATAFOLDER = Path(__file__).parent / 'data' / 'ria_test'


def test_exposuremodel(db_session):
    exposure_model = ExposureService.import_from_file(
        db_session,
        file_path=DATAFOLDER / 'exposure_test.xml',
        name='Test Exposure Model'
    )

    exposure_raw = pd.read_csv(DATAFOLDER / 'exposure_test.csv')

    _, buffer_csv = ExposureService.export_to_buffer(
        db_session, exposure_model.oid)

    exposure_db = pd.read_csv(buffer_csv)

    cols = ['lon', 'lat', 'structural', 'contents', 'business_interruption',
            'nonstructural', 'day', 'night', 'number', 'transit']

    for col in cols:
        print(f'raw: {exposure_raw[col].sum()}, db: {exposure_db[col].sum()}')
        np.testing.assert_almost_equal(
            exposure_raw[col].sum(),
            exposure_db[col].sum(),
            decimal=6,
            err_msg=f"Column {col} does not match between raw and db data.")

    for col in ['taxonomy', 'Canton', 'CantonGemeinde']:
        assert set(exposure_raw[col].unique()) == set(
            exposure_db[col].unique()), f"{col} values do not match."

    exposure_raw[['lon', 'lat', 'CantonGemeinde']].sort_values(
        by=['lon', 'lat', 'CantonGemeinde']).reset_index(drop=True).equals(
        exposure_db[['lon', 'lat', 'CantonGemeinde']].sort_values(
            by=['lon', 'lat', 'CantonGemeinde']).reset_index(drop=True)
    )


def test_fragilitymodel(db_session):
    fragility_model = FragilityService.import_from_file(
        db_session,
        file_path=DATAFOLDER / 'fragility_test.xml',
        name='Test Fragility Model'
    )

    buffer = FragilityService.export_to_buffer(
        db_session, fragility_model.oid)

    with open(DATAFOLDER / 'fragility_test.xml', 'r') as f:
        fragility_raw = f.read()

    # Compare the raw XML with the exported buffer
