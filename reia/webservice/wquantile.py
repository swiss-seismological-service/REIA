import numpy as np
from scipy.stats import beta


def add_missing_zeroes(values, weights):
    zero_weight = 1 - np.sum(weights)
    v = np.append(
        values, [0])
    w = np.append(
        weights, [zero_weight])
    return (v, w)


def weighted_quantile(values, quantiles, weights):
    """
    Calculates Quantiles of a weighted list of samples.

    Implementation for C=0 of:
    https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method

    If the sum of the weights is smaller than 1, it is assumed that
    the data is sparse and the remaining data is filled up with 0.

    :param values: array-like with data
    :param quantiles: array-like with many quantiles needed
    :param weights: array-like of the same length as `array`
    :return: numpy.array with computed quantiles.
    """

    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)

    sum_weight = np.sum(weights)

    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
        'Quantiles should be in [0, 1]'

    if sum_weight < 1:
        values, weights = add_missing_zeroes(values, weights)

    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weighted_quantiles = np.cumsum(weights)  # C=0

    return np.interp(quantiles, weighted_quantiles, values)


def wquantile_generic(values, quantiles, cdf_gen, weights):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)

    sum_weight = np.sum(weights)

    if sum_weight != 1 and sum_weight < 1:
        values, weights = add_missing_zeroes(values, weights)

    nw = sum(weights)**2 / sum(weights**2)
    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weights = weights / sum(weights)
    cdf_probs = np.cumsum(np.insert(weights, 0, [0]))
    res = []
    for prob in quantiles:
        cdf = cdf_gen(nw, prob)
        q = cdf(cdf_probs)
        w = q[1:] - q[:-1]
        res.append(np.sum(w * values))
    return res


def whdquantile(values, quantiles, weights):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    def cdf_gen_whd(n, p):
        return lambda x: beta.cdf(x, (n + 1) * p, (n + 1) * (1 - p))
    return wquantile_generic(values, quantiles, cdf_gen_whd, weights)


def type_7_cdf(quantiles, n, p):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    h = p * (n - 1) + 1
    u = np.maximum((h - 1) / n, np.minimum(h / n, quantiles))
    return u * n - h + 1


def wquantile(values, quantiles, weights):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    def cdf_gen_t7(n, p):
        return lambda x: type_7_cdf(x, n, p)
    return wquantile_generic(values, quantiles, cdf_gen_t7, weights)
