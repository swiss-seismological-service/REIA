from pathlib import Path

import pytest

from reia.cli import (add_exposure, add_exposure_geometries,
                      delete_exposure_geometries)
from reia.repositories.asset import (AggregationGeometryRepository,
                                     AggregationTagRepository)

DATAFOLDER = Path(__file__).parent / 'data'


@pytest.fixture(scope='module')
def exposure_with_geoms(db_session):
    exposure_id = add_exposure(DATAFOLDER / 'ria_test'
                               / 'exposure_test.xml', 'test')

    add_exposure_geometries(exposure_id, 'CantonGemeinde', 'tag',
                            DATAFOLDER / 'geometries' / 'municipalities.shp')
    return exposure_id


def test_geometries(exposure_with_geoms, db_session):
    geometries = AggregationGeometryRepository.get_by_exposuremodel(
        db_session, exposure_with_geoms)
    aggregationtags = AggregationTagRepository.get_by_exposuremodel(
        db_session, exposure_with_geoms)

    churwalden = next((a for a in aggregationtags if a.name == 'GR3911'), None)
    assert churwalden is not None
    assert len(churwalden.geometries) == 1

    churwalden_geometry = next(
        (g for g in geometries if g.aggregationtag_oid == churwalden.oid),
        None)
    assert churwalden_geometry is not None
    assert churwalden_geometry.name == 'Churwalden'

    nendaz = next(
        (a for a in geometries if a.name == 'Nendaz'), None)
    assert nendaz is not None


def test_geometry_deletion(exposure_with_geoms, db_session):
    delete_exposure_geometries(exposure_with_geoms, 'CantonGemeinde')
    geometries = AggregationGeometryRepository.get_by_exposuremodel(
        db_session, exposure_with_geoms)
    assert len(geometries) == 0
