from pyspark import RDD
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from pyspark import SparkContext

from analysis_functions import is_numeric


def scatter_plot_with_song_hotness(rdd, metadata_songs_rdd, number_of_points=500):
    """
    :type rdd: RDD
    :type metadata_songs_rdd: RDD
    """
    hotness = metadata_songs_rdd.map(lambda x: (x[0], x[1]['song_hotttnesss'][0])).filter(lambda x: is_numeric(x[1]))
    joined_rdds = rdd.join(hotness)
    points = joined_rdds.sample(False, float(number_of_points)/float(joined_rdds.count())).map(lambda x: x[1]).collect()
    x = [i[0] for i in points]
    y = [i[1] for i in points]
    plt.plot(x, y, '.')
    plt.show()


def scatter_plot_with_song_hotness_and_year(rdd, metadata_songs_rdd, music_brainz_rdd, number_of_points=500):
    hotness = metadata_songs_rdd.map(lambda x: (x[0], x[1]['song_hotttnesss'][0])).filter(lambda x: is_numeric(x[1]))
    year = music_brainz_rdd.map(lambda x: (x[0], x[1]['year'][0])).filter(lambda x: not (int(x[1]) == 0))
    joined_rdds = rdd.join(hotness).join(year)
    points = joined_rdds.sample(False, float(number_of_points) / float(joined_rdds.count())).map(
        lambda x: x[1]).collect()
    x = [i[0][0] for i in points]
    y = [i[0][1] for i in points]
    z = [i[1] for i in points]
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(x, y, z)
    plt.show()


def correlation_artist_hotness(sc):
    metadata_songs_rdd = sc.pickleFile(r'../data/metadata-songs')
    artist_hotness = metadata_songs_rdd.map(
        lambda x: (x[0], x[1]['song_hotttnesss'][0])
    ).filter(lambda x: is_numeric(x[1]))
    scatter_plot_with_song_hotness(artist_hotness, metadata_songs_rdd)


def correlation_tempo(sc):
    analysis = sc.pickleFile(r'../data/analysis-songs')
    metadata_songs_rdd = sc.pickleFile(r'../data/metadata-songs')
    tempo = analysis.map(lambda x: (x[0], x[1]['tempo'][0])).filter(lambda x: is_numeric(x[1]))
    scatter_plot_with_song_hotness(tempo, metadata_songs_rdd)


def correlation_tempo_and_year(sc):
    music_brainz_rdd = sc.pickleFile(r'../data/musicbrainz-songs')
    analysis_songs_rdd = sc.pickleFile(r'../data/analysis-songs')
    metadata_songs_rdd = sc.pickleFile(r'../data/metadata-songs')
    tempo = analysis_songs_rdd.map(lambda x: (x[0], x[1]['tempo'][0])).filter(lambda x: is_numeric(x[1]))
    scatter_plot_with_song_hotness_and_year(tempo, metadata_songs_rdd, music_brainz_rdd)


if __name__ == '__main__':
    sc = SparkContext(appName='testCorrelation')
    correlation_tempo_and_year(sc)
    sc.stop()
