from pyspark import RDD
import matplotlib.pyplot as plt
from pyspark import SparkContext

from analysis_functions import is_numeric


def song_hotness_correlations(rdd, metadata_songs_rdd, number_of_points=500):
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


def correlation_artist_hotness(sc):
    metadata_songs_rdd = sc.pickleFile(r'C:\Users\marc_\Documents\Prog\ADA_Group\project\data\metadata-songs')
    artist_hotness = metadata_songs_rdd.map(
        lambda x: (x[0], x[1]['song_hotttnesss'][0])
    ).filter(lambda x: is_numeric(x[1]))
    song_hotness_correlations(artist_hotness, metadata_songs_rdd)


def correlation_tempo(sc):
    analysis = sc.pickleFile(r'C:\Users\marc_\Documents\Prog\ADA_Group\project\data\analysis-songs')
    metadata_songs_rdd = sc.pickleFile(r'C:\Users\marc_\Documents\Prog\ADA_Group\project\data\metadata-songs')
    tempo = analysis.map(lambda x: (x[0], x[1]['tempo'][0])).filter(lambda x: is_numeric(x[1]))
    song_hotness_correlations(tempo, metadata_songs_rdd)

if __name__ == '__main__':
    sc = SparkContext(appName='testCorrelation')
    correlation_artist_hotness(sc)
    sc.stop()
