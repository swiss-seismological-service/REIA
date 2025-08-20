from pathlib import Path

import numpy as np
import pandas as pd

from reia.services.exposure import ExposureService
from reia.services.fragility import FragilityService
from reia.services.vulnerability import VulnerabilityService

DATAFOLDER = Path(__file__).parent / 'data' / 'ria_test'


def compare_xml_semantically(xml1_str, xml2_str):
    """Compare XML by extracting and comparing numeric data arrays."""
    import re

    # Extract all numeric sequences from both XMLs
    def extract_numbers(xml_str):
        # Find all sequences of space-separated numbers
        number_pattern = r'(?:(?:\d+\.?\d*)\s*)+'
        matches = re.findall(number_pattern, xml_str)

        all_numbers = []
        for match in matches:
            # Convert each number sequence to float array
            nums = [float(x) for x in match.strip().split() if x.strip()]
            if len(nums) > 2:  # Only consider sequences with multiple numbers
                all_numbers.extend(nums)

        return np.array(all_numbers)

    numbers1 = extract_numbers(xml1_str)
    numbers2 = extract_numbers(xml2_str)

    if len(numbers1) != len(numbers2):
        return False

    return np.allclose(numbers1, numbers2, rtol=1e-10, atol=1e-10)


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

    exposure_raw_sorted = exposure_raw.sort_values(
        by=['id']).reset_index(drop=True).reindex(
        sorted(exposure_raw.columns), axis=1).drop(columns=['id'])

    exposure_db_sorted = exposure_db.sort_values(
        by=['id']).reset_index(drop=True).reindex(
        sorted(exposure_db.columns), axis=1).drop(
            columns=['id']).astype(exposure_raw_sorted.dtypes.to_dict())

    assert exposure_raw_sorted.equals(exposure_db_sorted), \
        "Database-generated exposure model does not match original CSV data"


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

    # Get the exported XML from the buffer
    buffer.seek(0)  # Reset buffer position to beginning
    fragility_db = buffer.read()

    # Compare the raw XML with the exported buffer XML semantically
    assert compare_xml_semantically(fragility_raw, fragility_db), \
        "Database-generated XML does not match original XML semantically"


def test_vulnerabilitymodel(db_session):
    vulnerability_model = VulnerabilityService.import_from_file(
        db_session,
        file_path=DATAFOLDER / 'vulnerability_test.xml',
        name='Test Vulnerability Model'
    )

    buffer = VulnerabilityService.export_to_buffer(
        db_session, vulnerability_model.oid)

    with open(DATAFOLDER / 'vulnerability_test.xml', 'r') as f:
        vulnerability_raw = f.read()

    # Get the exported XML from the buffer
    buffer.seek(0)  # Reset buffer position to beginning
    vulnerability_db = buffer.read()

    # Compare the raw XML with the exported buffer XML semantically
    assert compare_xml_semantically(vulnerability_raw, vulnerability_db), \
        "Database-generated XML does not match original XML semantically"
