import json
from pathlib import Path

import numpy as np
import pytest


def load_expected_response(endpoint_name):
    """Load expected response from JSON file."""
    data_path = Path(__file__).parent / "data" / \
        "webservice" / f"{endpoint_name}.json"
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
async def test_riskassessment_endpoint(test_client, risk_assessment):
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
async def test_loss_endpoint(test_client):
    """Test /loss endpoint structure, keys and values."""
    response = await test_client.get("/v1/loss/1/structural/Canton")
    assert response.status_code == 200

    expected_data = load_expected_response("loss")
    compare_responses(
        response.json(),
        expected_data,
        check_values=True,
        tolerance=0.01)


@pytest.mark.asyncio
async def test_damage_endpoint(test_client):
    """Test /damage endpoint structure, keys and values."""
    response = await test_client.get("/v1/damage/2/structural/Canton")
    assert response.status_code == 200

    expected_data = load_expected_response("damage")
    compare_responses(
        response.json(),
        expected_data,
        check_values=True,
        tolerance=0.01)
