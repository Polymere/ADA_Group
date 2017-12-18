from src.read_data import Read
import sys
import re
import os
from pyspark import SparkContext

keys = [
    'analysis.bars_confidence',
    'analysis.bars_start',
    'analysis.beats_confidence',
    'analysis.beats_start',
    'analysis.sections_confidence',
    'analysis.sections_start',
    'analysis.segments_confidence',
    'analysis.segments_loudness_max',
    'analysis.segments_loudness_max_time',
    'analysis.segments_loudness_start',
    'analysis.segments_pitches',
    'analysis.segments_start',
    'analysis.segments_timbre',
    'analysis.songs',
    'analysis.tatums_confidence',
    'analysis.tatums_start',
    'metadata.artist_terms',
    'metadata.artist_terms_freq',
    'metadata.artist_terms_weight',
    'metadata.similar_artists',
    'metadata.songs',
    'musicbrainz.artist_mbtags',
    'musicbrainz.artist_mbtags_count',
    'musicbrainz.songs'
]

if __name__ == "__main__":
    """
    Program takes two arguments: the data folder of the million song subset and the output folders of the rdds
    """
    sc = SparkContext(appName="ExtractRDDsFromMillionSongSubset")
    for k in keys:
        print("Processing " + k)
        reader = Read(sys.argv[1], k, print_progress=True)
        rdd = sc.parallelize(reader)
        rdd.saveAsPickleFile(os.path.join(sys.argv[2], re.sub('\\.', '-', k)), 1000)
    sc.stop()
