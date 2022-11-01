import pytest
from dateutil.parser import parse
from esloss.datamodel.asset import Asset, AssetCollection, Site
from esloss.datamodel.lossmodel import Calculation, LossConfig, LossModel
from esloss.datamodel.vulnerability import (VulnerabilityFunction,
                                            VulnerabilityModel)


@pytest.mark.usefixtures("test_data")
class TestAPIGet:

    def test_get_exposure(self, client_class, db_class):

        response_db = db_class.query(AssetCollection).first()._asdict()
        response_db['assets_count'] = db_class.query(Asset).filter(
            Asset._assetcollection_oid == response_db['_oid']).count()
        response_db['sites_count'] = db_class.query(Site).filter(
            Site._assetcollection_oid == response_db['_oid']).count()

        response_get = client_class.get('/api/v1/assetcollection')
        assert response_get.status == '200 OK'

        response_json = response_get.json[0]
        response_json['creationinfo_creationtime'] = parse(
            response_json['creationinfo_creationtime'], ignoretz=True)
        assert response_json == response_db

    def test_get_vulnerability(self, client_class, db_class):

        response_db = db_class.query(VulnerabilityModel).first()._asdict()
        response_db['functions_count'] = db_class.query(VulnerabilityFunction).filter(
            VulnerabilityFunction._vulnerabilitymodel_oid == response_db['_oid']).count()

        response_get = client_class.get('/api/v1/vulnerabilitymodel')
        assert response_get.status == '200 OK'

        response_json = response_get.json[0]
        assert response_json == response_db

    def test_get_lossmodel(self, client_class, db_class):

        response_db = db_class.query(LossModel).first()._asdict()
        response_db['_vulnerabilitymodels_oids'] = sorted([2, 1])
        response_db['calculations_count'] = db_class.query(Calculation).filter(
            Calculation._lossmodel_oid == response_db['_oid']).count()

        response_get = client_class.get('/api/v1/lossmodel')
        assert response_get.status == '200 OK'

        response_json = response_get.json[0]

        assert response_json == response_db

    def test_get_lossconfig(self, client_class, db_class):

        response_db = db_class.query(LossConfig).first()._asdict()

        response_get = client_class.get('/api/v1/lossconfig')
        assert response_get.status == '200 OK'

        response_json = response_get.json[0]
        assert response_json == response_db

    def test_get_calculation(self, client_class, db_class):

        response_db = db_class.query(Calculation).first()._asdict()

        response_get = client_class.get('/api/v1/calculation')
        assert response_get.status == '200 OK'

        response_json = response_get.json[0]
        response_json['creationinfo_creationtime'] = parse(
            response_json['creationinfo_creationtime'], ignoretz=True)
        response_json['timestamp_starttime'] = parse(
            response_json['timestamp_starttime'], ignoretz=True)
        assert response_json == response_db
