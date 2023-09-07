from pathlib import Path

import pytest

from reia.cli import (add_exposure, add_exposure_geometries,
                      delete_exposure_geometries)
from reia.db import crud

DATAFOLDER = Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def exposure_with_geoms(db_session):
    exposure_id = add_exposure(DATAFOLDER / 'ria_test'
                               / 'exposure_test.xml', 'test')

    add_exposure_geometries(exposure_id, 'CantonGemeinde', DATAFOLDER
                            / 'geometries' / 'municipalities.shp')
    return exposure_id


def test_geometries(exposure_with_geoms, db_session):
    exposure = crud.read_asset_collection(exposure_with_geoms, db_session)

    assert len(exposure.aggregationgeometries) == 3

    aggregationtags = exposure.aggregationtags
    churwalden = next((a for a in aggregationtags if a.name == 'GR3911'), None)

    assert churwalden is not None
    assert len(churwalden.geometries) == 1

    geometry = churwalden.geometries[0]

    assert geometry.name == 'Churwalden'

    nendaz = next(
        (a for a in exposure.aggregationgeometries if a.name == 'Nendaz'),
        None)
    assert nendaz is not None


def test_geometry_deletion(exposure_with_geoms, db_session):
    delete_exposure_geometries(exposure_with_geoms, 'CantonGemeinde')
    exposure = crud.read_asset_collection(exposure_with_geoms, db_session)
    assert len(exposure.aggregationgeometries) == 0
