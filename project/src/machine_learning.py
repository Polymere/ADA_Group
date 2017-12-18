from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
import numpy as np
from pyspark import RDD
from cluster_utils import get_rdd
from pyspark import SparkContext


def get_metadata_features(analysis_songs_rdd, metadata_songs_rdd):
    analysis_features = analysis_songs_rdd. \
        map(lambda x: (x[0],
                       [
                           x[1]['duration'][0],
                           x[1]['key'][0],
                           x[1]['loudness'][0],
                           x[1]['tempo'][0],
                           x[1]['time_signature'][0]
                       ])
            )

    hotness = metadata_songs_rdd.map(lambda x: (x[0], x[1]['song_hotttnesss'][0]))

    return hotness, analysis_features


def join_features(feat1, feat2):
    return feat1.join(feat2).map(lambda x: (x[0], x[1][0] + x[1][1]))


def get_pitch_features(segments_pitches_rdd):
    return segments_pitches_rdd.map(lambda x: (x[0], [i for i in np.ravel(np.mean(x[1], axis=0))]))


def build_dataframe(hotness, features):
    """

    :type hotness: RDD
    :type features: RDD
    :return: RDD
    """
    hotness = hotness.filter(lambda x: not np.isnan(x[1]) and not x[1] == 0)
    features.map(lambda x: (x[0], [0.0 if np.isnan(i) else float(i) for i in x[1]]))
    rdd = hotness.join(features)
    rdd = rdd.map(lambda x: LabeledPoint(float(x[1][0]), x[1][1]))
    return rdd


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
        print("iteration: {0}, rmse: {1}".format(str(i + 1), str(rmse)))
        out.append(rmse)
    return out, weights


def classify(dataframe, nb_class=10):
    def _helper(label, nb_class):
        return float(int(label / (1.0 / nb_class))) / nb_class

    return dataframe.map(lambda x: LabeledPoint(_helper(x.label, nb_class), x.features))


if __name__ == '__main__':
    sc = SparkContext()
    hotness, analysis_features = get_metadata_features(get_rdd(sc, 'analysis-songs'), get_rdd(sc, 'metadata-songs'))
    pitch_rdd = get_pitch_features(get_rdd(sc, 'analysis-segments_pitches'))

    features = join_features(analysis_features, pitch_rdd)
    dataframe = build_dataframe(hotness, features)
    visualize_descent(
        lambda data, weights: LinearRegressionWithSGD.train(data, iterations=10, step=0.000001, initialWeights=weights),
        dataframe, 100)
