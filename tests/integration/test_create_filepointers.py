
import re
from app.blueprints.api.utils import (
    create_exposure_csv, create_exposure_xml, create_hazard_ini, create_risk_ini, create_vulnerability_xml)

import pytest


@pytest.mark.usefixtures("test_data_from_files")
class TestCreateFiles:

    def test_create_exposure_xml(self, test_query):
        result_string = create_exposure_xml(
            test_query['asset_collection']).read()

        with open('tests/data/exposure.xml') as file:
            control_string = file.read()

        result_string = re.sub(r"[\n\t\s]*", "", result_string)
        control_string = re.sub(r"[\n\t\s]*", "", control_string)
        assert result_string == control_string

    def test_create_vulnerability_xml(self, test_query):
        result_string = create_vulnerability_xml(
            test_query['vulnerability_model']).read()

        with open('tests/data/structural_vulnerability.xml') as file:
            control_string = file.read()

        result_string = re.sub(r"[\n\t\s]*", "", result_string)
        control_string = re.sub(r"[\n\t\s]*", "", control_string)
        assert result_string == control_string

    def test_create_hazard_ini(self, test_query):
        result_string = create_hazard_ini(test_query['loss_model']).read()

        with open('tests/integration/data/hazard_test.ini') as file:
            control_string = file.read()

        result_string = re.sub(r"[\n\t\s]*", "", result_string)
        control_string = re.sub(r"[\n\t\s]*", "", control_string)
        assert result_string == control_string

    def test_create_risk_ini(self, test_query):
        result_string = create_risk_ini(test_query['loss_model']).read()

        with open('tests/integration/data/risk_test.ini') as file:
            control_string = file.read()

        result_string = re.sub(r"[\n\t\s]*", "", result_string)
        control_string = re.sub(r"[\n\t\s]*", "", control_string)
        assert result_string == control_string

    def test_create_exposure_csv(self, test_query):
        result_string = create_exposure_csv(
            test_query['asset_collection'].assets).read()

        with open('tests/integration/data/exposure_assets.csv') as file:
            control_string = file.read()

        result_string = re.sub(r"[\n\t\s]*", "", result_string)
        control_string = re.sub(r"[\n\t\s]*", "", control_string)
        print(result_string)
        assert result_string == control_string
