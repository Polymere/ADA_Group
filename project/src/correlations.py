from pyspark import SparkContext
from cluster_utils import get_rdd
import analysis_functions
from pyspark.mllib.stat import Statistics
import pickle

# OUT_PATH = '/buffer/mrp_buffer/histograms/'
OUT_PATH = 'test/'


def correlation_preparation(rdd, field_name):
    return rdd.map(lambda x: (x[0], x[1][field_name][0])) \
        .filter(lambda x: analysis_functions.is_numeric(x[1])) \
        .filter(lambda x: float(x[1]) != 0.0)


def calculate_correlation(rdd1, rdd2, method="pearson"):
    # we join to keep only the element we have in common
    rdd = rdd1.join(rdd2)
    return Statistics.corr(rdd.map(lambda x: x[1][0]), rdd.map(lambda x: x[1][1]), method=method)


if __name__ == "__main__":
    sc = SparkContext(appName='CorrelationOfData')
    DATA_DIR = 'data/'
    music_brainz_rdd = sc.pickleFile(DATA_DIR + 'musicbrainz-songs')
    analysis_songs_rdd = sc.pickleFile(DATA_DIR + 'analysis-songs')
    metadata_songs_rdd = sc.pickleFile(DATA_DIR + 'metadata-songs')
    # music_brainz_rdd = get_rdd('musicbrainz-songs', sc)
    # analysis_songs_rdd = get_rdd('analysis-songs', sc)
    # metadata_songs_rdd = get_rdd('metadata-songs', sc)
    data = {
        'hotness': correlation_preparation(metadata_songs_rdd, 'song_hotttnesss'),
        'duration': correlation_preparation(analysis_songs_rdd, 'duration'),
        'key': correlation_preparation(analysis_songs_rdd, 'key'),
        'loudness': correlation_preparation(analysis_songs_rdd, 'loudness'),
        'tempo': correlation_preparation(analysis_songs_rdd, 'tempo'),
        'time_signature': correlation_preparation(analysis_songs_rdd, 'time_signature'),
        'latitude': correlation_preparation(metadata_songs_rdd, 'artist_latitude'),
        'longitude': correlation_preparation(metadata_songs_rdd, 'artist_longitude'),
        'year': correlation_preparation(music_brainz_rdd, 'year'),
    }

    correlations = {}
    for i in ['duration', 'key', 'loudness', 'tempo', 'time_signature', 'latitude', 'longitude', 'year']:
        correlations[i] = calculate_correlation(data['hotness'], data[i])

    with open(OUT_PATH + 'correlations-global', 'wb') as out_file:
        pickle.dump(correlations, out_file)
       