import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import select

from reia.datamodel import AggregationTag, LossValue, riskvalue_aggregationtag
from reia.schemas.enums import ECalculationType, ELossCategory
from reia.webservice.wquantile import weighted_quantile


def load_expected_response(endpoint_name):
    """Load expected response from JSON file."""
    data_path = Path(__file__).parent / "data" / f"{endpoint_name}.json"
    with open(data_path, 'r') as f:
        return json.load(f)


def compare_responses(
        actual,
        expected,
        path="",
        check_values=False,
        tolerance=0.01):
    """Compare responses recursively - structure/keys and optionally values."""
    match (expected, actual):
        case (dict(), dict()):
            # Check all expected keys exist in actual
            missing_keys = set(expected.keys()) - set(actual.keys())
            assert not missing_keys, f"Missing keys {missing_keys} at {path}"

            # Recursively compare nested structures
            for key in expected.keys():
                if key in actual:
                    compare_responses(actual[key], expected[key],
                                      f"{path}.{key}", check_values, tolerance)

        case (list(), list()):
            assert len(actual) > 0, f"Actual list is empty at {path}"
            # For lists, compare up to the minimum length
            min_len = min(len(actual), len(expected))
            # sort both lists to ensure order doesn't affect comparison
            actual.sort(key=lambda x: json.dumps(x, sort_keys=True))
            expected.sort(key=lambda x: json.dumps(x, sort_keys=True))
            for i in range(min_len):
                compare_responses(actual[i], expected[i],
                                  f"{path}[{i}]", check_values, tolerance)

        case (int() | float(), int() | float()) if check_values:
            # Only compare values if check_values is True
            np.testing.assert_allclose(
                actual, expected, rtol=tolerance, atol=tolerance,
                err_msg=f"Value mismatch at {path}: expected "
                f"{expected}, got {actual}")

        case _:
            # For non-numeric values when check_values=True, or any other case,
            # we just verify the structure exists (which we've already done
            # above)
            pass


@pytest.mark.asyncio
async def test_riskassessment_endpoint(test_client, example_risk_assessment):
    """Test /riskassessment endpoint structure and keys."""
    response = await test_client.get("/v1/riskassessment")
    assert response.status_code == 200

    compare_responses(response.json(),
                      load_expected_response("riskassessment"))


@pytest.mark.asyncio
async def test_calculation_endpoint(test_client):
    """Test /calculation endpoint structure and keys."""
    response = await test_client.get("/v1/calculation")
    assert response.status_code == 200

    compare_responses(response.json(), load_expected_response("calculation"))


@pytest.mark.asyncio
async def test_loss_endpoint(test_client, example_loss_calculation):
    """Test /loss endpoint structure, keys and values."""
    response = await test_client.get(
        f"/v1/loss/{example_loss_calculation.oid}/structural/state")
    assert response.status_code == 200, response.text

    expected_data = load_expected_response("loss")

    compare_responses(
        response.json(),
        expected_data,
        check_values=True,
        tolerance=0.01)


@pytest.mark.asyncio
async def test_damage_endpoint(test_client, example_damage_calculation):
    """Test /damage endpoint structure, keys and values."""
    response = await test_client.get(
        f"/v1/damage/{example_damage_calculation.oid}/structural/state")
    assert response.status_code == 200, response.text

    expected_data = load_expected_response("damage")

    compare_responses(
        response.json(),
        expected_data,
        check_values=True,
        tolerance=0.01)


async def calculate_statistics_python(
        db_session,
        calculation_id,
        aggregation_type,
        loss_category):
    """
    Calculate loss statistics using Python (old method).

    This mimics the REIA-ws approach of fetching raw data and
    calculating statistics in Python rather than in the database.
    """
    # Map string category to enum
    loss_category_enum = ELossCategory[loss_category.upper()]

    # Query raw loss data with aggregation tags
    stmt = select(
        LossValue.loss_value,
        LossValue.weight,
        AggregationTag.name.label('tag_name')
    ).select_from(LossValue).join(
        riskvalue_aggregationtag,
        (riskvalue_aggregationtag.c.riskvalue == LossValue._oid)
        & (riskvalue_aggregationtag.c.losscategory == LossValue.losscategory)
        & (riskvalue_aggregationtag.c._calculation_oid
           == LossValue._calculation_oid)
    ).join(
        AggregationTag,
        (AggregationTag._oid == riskvalue_aggregationtag.c.aggregationtag)
        & (AggregationTag.type == riskvalue_aggregationtag.c.aggregationtype)
    ).where(
        (LossValue._calculation_oid == calculation_id)
        & (LossValue.losscategory == loss_category_enum)
        & (LossValue._type == ECalculationType.LOSS)
        & (AggregationTag.type == aggregation_type)
    )

    # Execute query and convert to DataFrame
    result = await db_session.execute(stmt)
    rows = result.fetchall()
    columns = result.keys()
    df = pd.DataFrame(rows, columns=columns)

    if df.empty:
        return []

    # Calculate statistics for each aggregation tag
    statistics = []

    for tag_name in df['tag_name'].unique():
        tag_data = df[df['tag_name'] == tag_name]

        values = tag_data['loss_value'].values
        weights = tag_data['weight'].values

        # Calculate weighted mean
        loss_mean = float(np.sum(weights * values))

        # Calculate weighted percentiles
        pc10, pc90 = weighted_quantile(values, [0.1, 0.9], weights)

        statistics.append({
            'category': loss_category,
            'tag': [tag_name],
            'loss_mean': round(float(loss_mean), 5),
            'loss_pc10': round(float(pc10), 5),
            'loss_pc90': round(float(pc90), 5)
        })

    # Sort by tag name for consistent comparison
    statistics.sort(key=lambda x: x['tag'][0])

    return statistics


@pytest.mark.asyncio
async def test_loss_statistics_python_vs_database(
        test_client,
        async_db_session,
        example_loss_calculation):
    """
    Compare Python-based statistics calculation (old method) with
    database-based calculation (new method).
    """
    calculation_id = example_loss_calculation.oid
    aggregation_type = 'state'
    loss_category = 'structural'

    # Calculate statistics using Python (old method)
    python_stats = await calculate_statistics_python(
        async_db_session,
        calculation_id,
        aggregation_type,
        loss_category
    )

    # Get statistics from database endpoint (new method)
    response = await test_client.get(
        f"/v1/loss/{calculation_id}/{loss_category}/{aggregation_type}")
    assert response.status_code == 200, response.text

    database_stats = response.json()

    # Compare results
    assert len(python_stats) == len(database_stats), \
        f"Different number of results: Python={len(python_stats)}, " \
        f"Database={len(database_stats)}"

    # Sort both for consistent comparison
    python_stats.sort(key=lambda x: x['tag'][0])
    database_stats.sort(key=lambda x: x['tag'][0])

    # Compare each statistic
    for py_stat, db_stat in zip(python_stats, database_stats):
        assert py_stat['tag'] == db_stat['tag'], \
            f"Tag mismatch: {py_stat['tag']} vs {db_stat['tag']}"

        assert py_stat['category'] == db_stat['category'], \
            f"Category mismatch: {py_stat['category']} vs {db_stat['category']}"

        # Compare numerical values with tolerance
        np.testing.assert_allclose(
            py_stat['loss_mean'], db_stat['loss_mean'],
            rtol=0.01, atol=0.01,
            err_msg=f"loss_mean mismatch for tag {py_stat['tag']}")

        np.testing.assert_allclose(
            py_stat['loss_pc10'], db_stat['loss_pc10'],
            rtol=0.01, atol=0.01,
            err_msg=f"loss_pc10 mismatch for tag {py_stat['tag']}")

        np.testing.assert_allclose(
            py_stat['loss_pc90'], db_stat['loss_pc90'],
            rtol=0.01, atol=0.01,
            err_msg=f"loss_pc90 mismatch for tag {py_stat['tag']}")
