from pathlib import Path

import pytest

from reia.repositories.asset import (AggregationGeometryRepository,
                                     AggregationTagRepository)
from reia.services.exposure import (ExposureService,
                                    add_geometries_from_shapefile)

DATAFOLDER = Path(__file__).parent / 'data'


@pytest.fixture()
def exposure_with_geoms(db_session):
    exposure = ExposureService.import_from_file(db_session,
                                                DATAFOLDER / 'ria_test'
                                                / 'exposure_test.xml', 'test')
    add_geometries_from_shapefile(db_session,
                                  exposure.oid,
                                  DATAFOLDER
                                  / 'geometries'
                                  / 'municipalities.shp',
                                  'tag',
                                  'CantonGemeinde')

    return exposure.oid


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
    AggregationGeometryRepository.delete_by_exposuremodel(
        db_session, exposure_with_geoms, 'CantonGemeinde')
    geometries = AggregationGeometryRepository.get_by_exposuremodel(
        db_session, exposure_with_geoms)
    assert len(geometries) == 0
