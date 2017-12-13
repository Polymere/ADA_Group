from cluster_utils import get_rdd
from pyspark import SparkContext
import src.analysis_functions as analysis_functions
import pickle

OUT_PATH = '/buffer/mrp_buffer/histograms/'


def simple_hist_preparation(rdd, field_name):
    return rdd.map(lambda x: (x[0], x[1][field_name][0])).filter(lambda x: analysis_functions.is_numeric(x[1]))


def get_and_save_histogram(rdd, out_file_name):
    obj = analysis_functions.get_histogram(rdd.map(lambda x: x[1]))
    with open(OUT_PATH + out_file_name, 'wb') as out_file:
        pickle.dump(obj, out_file)


if __name__ == "__main__":
    sc = SparkContext(appName='GetHistogramsOfData')
    # DATA_DIR = 'data/'
    # music_brainz_rdd = sc.pickleFile(DATA_DIR + 'musicbrainz-songs')
    # analysis_songs_rdd = sc.pickleFile(DATA_DIR + 'analysis-songs')
    # metadata_songs_rdd = sc.pickleFile(DATA_DIR + 'metadata-songs')
    music_brainz_rdd = get_rdd('musicbrainz-songs', sc)
    analysis_songs_rdd = get_rdd('analysis-songs', sc)
    metadata_songs_rdd = get_rdd('metadata-songs', sc)
    hotness = simple_hist_preparation(metadata_songs_rdd, 'song_hotttnesss')
    danceability = simple_hist_preparation(analysis_songs_rdd, 'danceability')
    duration = simple_hist_preparation(analysis_songs_rdd, 'duration')
    energy = simple_hist_preparation(analysis_songs_rdd, 'energy')
    key = simple_hist_preparation(analysis_songs_rdd, 'key')
    loudness = simple_hist_preparation(analysis_songs_rdd, 'loudness')
    tempo = simple_hist_preparation(analysis_songs_rdd, 'tempo')
    time_signature = simple_hist_preparation(analysis_songs_rdd, 'time_signature')
    latitude = simple_hist_preparation(metadata_songs_rdd, 'artist_latitude')
    longitude = simple_hist_preparation(metadata_songs_rdd, 'artist_longitude')
    year = simple_hist_preparation(music_brainz_rdd, 'year')

    get_and_save_histogram(hotness, 'hotness-global')
    get_and_save_histogram(danceability, 'danceability-global')
    get_and_save_histogram(duration, 'duration-global')
    get_and_save_histogram(energy, 'energy-global')
    get_and_save_histogram(key, 'key-global')
    get_and_save_histogram(loudness, 'loudness-global')
    get_and_save_histogram(tempo, 'tempo-global')
    get_and_save_histogram(time_signature, 'time_signature-global')
    get_and_save_histogram(latitude, 'latitude-global')
    get_and_save_histogram(longitude, 'longitude-global')
    get_and_save_histogram(year, 'year-global')
