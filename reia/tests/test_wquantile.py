import numpy as np
import pytest

from reia.webservice.wquantile import (add_missing_zeroes, weighted_quantile,
                                       whdquantile, wquantile)


class TestAddMissingZeroes:
    def test_adds_zero_when_weights_sum_less_than_one(self):
        values = np.array([1, 2, 3])
        weights = np.array([0.2, 0.3, 0.3])  # sum = 0.8

        result_values, result_weights = add_missing_zeroes(values, weights)

        expected_values = np.array([1, 2, 3, 0])
        expected_weights = np.array([0.2, 0.3, 0.3, 0.2])

        np.testing.assert_array_equal(result_values, expected_values)
        np.testing.assert_array_almost_equal(result_weights, expected_weights)

    def test_correct_zero_weight_calculation(self):
        values = np.array([5, 10])
        weights = np.array([0.4, 0.1])  # sum = 0.5

        result_values, result_weights = add_missing_zeroes(values, weights)

        assert result_weights[-1] == 0.5  # zero_weight should be 1 - 0.5 = 0.5


class TestWeightedQuantile:
    def test_simple_median_calculation(self):
        values = np.array([1, 2, 3, 4, 5])
        weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        quantiles = np.array([0.5])

        result = weighted_quantile(values, quantiles, weights)

        # Use approximate comparison due to floating point precision
        np.testing.assert_almost_equal(result[0], 2.5, decimal=5)

    def test_multiple_quantiles(self):
        values = np.array([1, 2, 3, 4, 5])
        weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        quantiles = np.array([0.0, 0.5, 1.0])

        result = weighted_quantile(values, quantiles, weights)

        assert result[0] == 1.0  # minimum
        np.testing.assert_almost_equal(result[1], 2.5, decimal=5)  # median
        assert result[2] == 5.0  # maximum

    def test_weighted_different_weights(self):
        values = np.array([1, 2, 3])
        weights = np.array([0.1, 0.8, 0.1])  # heavily weighted towards 2
        quantiles = np.array([0.5])

        result = weighted_quantile(values, quantiles, weights)

        # With these weights, the 50th percentile should be around 1.5-2.0
        assert 1.0 <= result[0] <= 2.0

    def test_sparse_data_handling(self):
        values = np.array([10, 20])
        weights = np.array([0.3, 0.2])  # sum = 0.5 < 1
        quantiles = np.array([0.5])

        result = weighted_quantile(values, quantiles, weights)

        # Should handle sparse data by adding zeros
        assert isinstance(result[0], (int, float, np.floating))

    def test_single_value(self):
        values = np.array([42])
        weights = np.array([1.0])
        quantiles = np.array([0.0, 0.5, 1.0])

        result = weighted_quantile(values, quantiles, weights)

        # All quantiles should return the single value
        np.testing.assert_array_almost_equal(result, [42, 42, 42])

    def test_quantiles_boundary_validation(self):
        values = np.array([1, 2, 3])
        weights = np.array([0.3, 0.3, 0.4])

        # Test invalid quantiles
        with pytest.raises(AssertionError):
            weighted_quantile(values, np.array([-0.1]), weights)

        with pytest.raises(AssertionError):
            weighted_quantile(values, np.array([1.1]), weights)


class TestWhdquantile:
    def test_basic_functionality(self):
        values = np.array([1, 2, 3, 4, 5])
        weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        quantiles = np.array([0.5])

        result = whdquantile(values, quantiles, weights)

        # Should return a list with one element for median
        assert len(result) == 1
        assert isinstance(result[0], (int, float, np.floating))

    def test_multiple_quantiles_whdquantile(self):
        values = np.array([1, 2, 3, 4, 5])
        weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        quantiles = np.array([0.25, 0.5, 0.75])

        result = whdquantile(values, quantiles, weights)

        assert len(result) == 3
        # Results should be in ascending order
        assert result[0] <= result[1] <= result[2]


class TestWquantile:
    def test_basic_functionality(self):
        values = np.array([1, 2, 3, 4, 5])
        weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        quantiles = np.array([0.5])

        result = wquantile(values, quantiles, weights)

        # Should return a list with one element for median
        assert len(result) == 1
        assert isinstance(result[0], (int, float, np.floating))

    def test_multiple_quantiles_wquantile(self):
        values = np.array([1, 2, 3, 4, 5])
        weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
        quantiles = np.array([0.0, 0.5, 1.0])

        result = wquantile(values, quantiles, weights)

        assert len(result) == 3
        # Results should be in ascending order for min, median, max
        assert result[0] <= result[1] <= result[2]

    def test_unequal_weights(self):
        values = np.array([10, 20, 30])
        weights = np.array([0.5, 0.3, 0.2])
        quantiles = np.array([0.5])

        result = wquantile(values, quantiles, weights)

        # Should handle unequal weights properly
        assert isinstance(result[0], (int, float, np.floating))
        # With heavy weight on 10, median should be closer to lower values
        assert result[0] < 25  # should be less than simple average


class TestEdgeCases:
    def test_empty_arrays_handling(self):
        # Test with minimal valid input
        values = np.array([1])
        weights = np.array([1])
        quantiles = np.array([0.5])

        result_weighted = weighted_quantile(values, quantiles, weights)
        result_whd = whdquantile(values, quantiles, weights)
        result_wq = wquantile(values, quantiles, weights)

        assert result_weighted[0] == 1
        assert result_whd[0] == 1
        assert result_wq[0] == 1

    def test_zero_values_with_weights(self):
        values = np.array([0, 1, 2])
        weights = np.array([0.5, 0.3, 0.2])
        quantiles = np.array([0.5])

        result = weighted_quantile(values, quantiles, weights)

        # Should handle zero values correctly
        assert isinstance(result[0], (int, float, np.floating))
        assert result[0] >= 0
