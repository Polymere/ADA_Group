import numpy as np
from pyspark import RDD
from operator import add
import numpy as np


def is_array(x):
    if isinstance(x, np.ndarray) and len(x.shape) == 1:
        return True
    else:
        return False


def is_matrix(x):
    if isinstance(x, np.ndarray) and len(x.shape) == 2:
        return True
    else:
        return False


def is_numeric(x):
    if isinstance(x, np.ndarray):
        if x.dtype == np.dtype('float64') or x.dtype == np.dtype('int32'):
            return True
        else:
            return False

    elif isinstance(x, int):
        if np.isnan(x):
            return False
        else:
            return True

    elif isinstance(x, float):
        if np.isnan(x):
            return False
        else:
            return True

    return False


def flatten(x):
    return [i for i in np.ravel(x)]


def data_to_datatype_str(x):
    if is_array(x) and is_numeric(x):
        return ['array-' + str(x.dtype)]
    elif is_matrix(x) and is_numeric(x):
        return ['matrix-' + str(x.dtype)]
    elif isinstance(x, np.ndarray):
        return ['other-' + type(i).__name__ for i in x]
    else:
        ['other-' + type(x).__name__]


def get_data_types(rdd):
    """

    :param rdd:
    :return:
    :type rdd: RDD
    """
    data_types = rdd \
        .map(lambda t: t[1]) \
        .flatMap(data_to_datatype_str) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .collect()

    return data_types


def numeric_array_or_matrix_histogram(rdd, number_of_buckets=100):
    """
    :param rdd: rdd to analyse
    :param number_of_buckets: number of buckets to use for the histogram
    :return: (min, max, histogram, number_of_filtered_elements), where histogram is a tuple (buckets, values)
    :type rdd: RDD
    """

    filtered_rdd = rdd.map(lambda t: t[1]).filter(lambda x: is_array(x) or is_matrix(x)).filter(is_numeric)
    # number_of_filtered_elements = rdd.count() - filtered_rdd.count()

    flattened_rdd = filtered_rdd.flatMap(flatten)

    _min, _max, histogram = get_histogram(flattened_rdd, number_of_buckets)

    return _min, _max, histogram


def get_histogram(rdd, number_of_buckets=100):
    _min = rdd.min()
    _max = rdd.max()

    histogram = rdd.histogram(
        buckets=[
            i for i in
            np.arange(_min, _max, (float(_max) - float(_min)) / (float(number_of_buckets) + 1.0))
        ]
    )

    return _min, _max, histogram
