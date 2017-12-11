from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
import numpy as np
from pyspark import RDD


def format_metadata_features(analysis_songs_rdd, metadata_songs_rdd, music_brainz_rdd):
    analysis_features = analysis_songs_rdd. \
        map(lambda x: (x[0],
                       (
                           x[1]['danceability'][0],
                           x[1]['duration'][0],
                           x[1]['energy'][0],
                           x[1]['key'][0],
                           x[1]['loudness'][0],
                           x[1]['tempo'][0],
                           x[1]['time_signature'][0]
                       ))
            )
    metadata_features = metadata_songs_rdd. \
        map(lambda x: (x[0],
                       (
                           x[1]['artist_latitude'][0],
                           x[1]['artist_longitude'][0]
                       ))
            )
    musicbrainz_features = music_brainz_rdd. \
        map(lambda x: (x[0],
                       (
                           x[1]['year'][0],
                       ))
            )

    hotness = metadata_songs_rdd.map(lambda x: (x[0], x[1]['song_hotttnesss'][0]))

    return hotness, join_features(join_features(analysis_features, metadata_features), musicbrainz_features)


def join_features(feat1, feat2):
    return feat1.join(feat2).map(lambda x: (x[0], x[1][0] + x[1][1]))


def build_dataframe(hotness, features):
    """

    :type hotness: RDD
    :type features: RDD
    :return: RDD
    """
    hotness = hotness.filter(lambda x: not np.isnan(x[1]))
    rdd = hotness.join(features)
    rdd = rdd.map(lambda x: LabeledPoint(float(x[1][0]), _transforme_type_for_dataframe(x[1][1])))
    return rdd


def _transforme_type_for_dataframe(x):
    arr = np.array(x)
    nan = np.isnan(arr)
    zeros = np.logical_or(arr == 0.0, arr == 0)
    arr[nan] = 0.0
    return [float(i) for i in np.concatenate((arr, nan, zeros))]


def cross_validation(dataframe, algorithm, ratio=0.8, nb_validation=8, seed=0):
    out = []
    train, validation = dataframe.randomSplit((ratio, 1 - ratio), seed=seed)
    for i in range(nb_validation):
        model = algorithm(train)
        out.append(rmse_error(model, validation))
    return out


def rmse_error(model, dataframe):
    dataframe = dataframe.zipWithIndex()
    features = dataframe.map(lambda x: (x[1], x[0].features))
    y_actual = dataframe.map(lambda x: (x[1], x[0].label))
    y_predicted = features.map(lambda x: (x[0], model.predict(x[1])))
    n = y_actual.count()
    rmse = y_actual.join(y_predicted).map(lambda x: ((x[1][0] - x[1][1]) ** 2) / n).sum()
    return np.sqrt(rmse)


def visualize_descent(algorithm, dataframe, iterations=100):
    out = []
    weights = None
    for i in range(iterations):
        model = algorithm(dataframe, weights)
        weights = model.weights
        rmse = rmse_error(model, dataframe)
        print("iteration: {0}, rmse: {1}".format(str(i+1), str(rmse)))
        out.append(rmse)
    return out, weights


def classify(dataframe, nb_class=10):
    def _helper(label, nb_class):
        return float(int(label / (1.0 / nb_class))) / nb_class

    return dataframe.map(lambda x: LabeledPoint(_helper(x.label, nb_class), x.features))
