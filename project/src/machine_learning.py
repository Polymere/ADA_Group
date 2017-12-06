from pyspark.mllib.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SQLContext


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
    rdd = hotness.join(features)
    rdd = rdd.map(lambda x: (x[1][0], Vectors.dense(x[1][1])))
    dataframe = SQLContext.createDataFrame(rdd, ['label', 'features'])
    return dataframe


def linear_regression(dataframe):
    lr = LinearRegression(maxIter=100, regParam=0.001)
    model = lr.fit(dataframe)
    return model
