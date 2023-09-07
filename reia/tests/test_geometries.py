from pathlib import Path

import pytest

from reia.cli import add_exposure, add_exposure_geometries

# from reia.db import crud

DATAFOLDER = Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def exposure_with_geoms(db_session):
    exposure_id = add_exposure(DATAFOLDER / 'ria_test'
                               / 'exposure_test.xml', 'test')

    add_exposure_geometries(exposure_id, DATAFOLDER
                            / 'geometries' / 'municipalities.shp')
    return exposure_id


def test_geometries(exposure_with_geoms, db_session):
    assert True
