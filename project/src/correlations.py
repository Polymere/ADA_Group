from pyspark import SparkContext
from cluster_utils import get_rdd
import analysis_functions
from pyspark.mllib.stat import Statistics
import pickle

from sampling import EqualClassSampling

OUT_PATH = '/buffer/mrp_buffer/correlations/'


# OUT_PATH = 'test/'


def correlation_preparation(rdd, field_name):
    return rdd.map(lambda x: (x[0], x[1][field_name][0])) \
        .filter(lambda x: analysis_functions.is_numeric(x[1])) \
        .filter(lambda x: float(x[1]) != 0.0)


def calculate_correlation(rdd1, rdd2, method="pearson"):
    # we join to keep only the element we have in common
    rdd = rdd1.join(rdd2)
    n = rdd.count()
    if n == 0:
        return {'corr': 0, 'nb_elements': 0}

    if n == 1:
        return {'corr': 0, 'nb_elements': 1}

    return {
        'corr': Statistics.corr(rdd.map(lambda x: x[1][0]), rdd.map(lambda x: x[1][1]), method=method),
        'nb_elements': n
    }


def load_data(sc):
    # DATA_DIR = 'data/'
    # music_brainz_rdd = sc.pickleFile(DATA_DIR + 'musicbrainz-songs')
    # analysis_songs_rdd = sc.pickleFile(DATA_DIR + 'analysis-songs')
    # metadata_songs_rdd = sc.pickleFile(DATA_DIR + 'metadata-songs')
    music_brainz_rdd = get_rdd('musicbrainz-songs', sc)
    analysis_songs_rdd = get_rdd('analysis-songs', sc)
    metadata_songs_rdd = get_rdd('metadata-songs', sc)
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
    return data


def correlation_calculations():
    sc = SparkContext(appName='CorrelationOfData')
    data = load_data(sc)
    correlations = dict()
    for i in ['duration', 'key', 'loudness', 'tempo', 'time_signature', 'latitude', 'longitude', 'year']:
        correlations[i] = calculate_correlation(data['hotness'], data[i])

    with open(OUT_PATH + 'correlations-global', 'wb') as out_file:
        pickle.dump(correlations, out_file)

    correlations_per_year = dict()
    for year in range(1922, 2018):
        correlations_per_year[year] = dict()
        y = data['year'].filter(lambda x: x[1] == year)
        for i in ['duration', 'key', 'loudness', 'tempo', 'time_signature', 'latitude', 'longitude']:
            # filtering tuples of that year
            rdd = y.join(data[i]).map(lambda x: (x[0], x[1][1]))
            correlations_per_year[year][i] = calculate_correlation(data['hotness'], rdd)

    with open(OUT_PATH + 'correlations-per-year', 'wb') as out_file:
        pickle.dump(correlations, out_file)


def sampled_points():
    sc = SparkContext(appName='CorrelationOfData')
    sampler = EqualClassSampling()
    data = load_data(sc)
    for i in ['duration', 'key', 'loudness', 'tempo', 'time_signature', 'latitude', 'longitude', 'year']:
        # selecting the tuples in common
        rdd = data['hotness'].join(data[i])
        # finding the classes so that they each contain about the same number of elements
        hotness_rdd = rdd.map(lambda x: (x[0], x[1][0]))
        sampler.calculate_nb_elements_per_class(hotness_rdd)
        sampled_hotness_rdd = sampler.sample(hotness_rdd, 200)
        points = sampled_hotness_rdd.join(data[i]).map(lambda x: (x[1][0][0], x[1][1], x[1][0][1])).collect()
        with open(OUT_PATH + 'sampled-points-' + i, 'wb') as out_file:
            pickle.dump({'points': points, 'original_el_per_class': sampler.elements_per_class}, out_file)

    for year in range(1922, 2018):
        y = data['year'].filter(lambda x: x[1] == year)
        for i in ['duration', 'key', 'loudness', 'tempo', 'time_signature', 'latitude', 'longitude', 'year']:
            # selecting the tuples in common
            rdd = data['hotness'].join(data[i]).join(y).map(lambda x: (x[0], x[1][0]))
            if rdd.count() < 10:
                continue
            # finding the classes so that they each contain about the same number of elements
            hotness_rdd = rdd.map(lambda x: (x[0], x[1][0]))
            sampler.calculate_nb_elements_per_class(hotness_rdd)
            sampled_hotness_rdd = sampler.sample(hotness_rdd, 200)
            points = sampled_hotness_rdd.join(data[i]).map(lambda x: (x[1][0][0], x[1][1], x[1][0][1])).collect()
            with open(OUT_PATH + 'sampled-points-' + str(year) + '-' + i, 'wb') as out_file:
                pickle.dump({'points': points, 'original_el_per_class': sampler.elements_per_class}, out_file)


if __name__ == "__main__":
    sampled_points()
