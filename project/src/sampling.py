import math
from operator import add

import analysis_functions
from cluster_utils import get_rdd
from pyspark import SparkContext
import pickle
import sys

OUT_PATH = '/buffer/mrp_buffer/name_sampling/'
# OUT_PATH = 'test/'


class EqualClassSampling:
    def __init__(self, nb_class=10):
        self.elements_per_class = dict()
        self.class_divider = 1.0 / float(nb_class)
        self.nb_class = nb_class

    def get_class(self, hotness):
        return int(math.floor(hotness / self.class_divider))

    def calculate_nb_elements_per_class(self, hotness_rdd):
        self.elements_per_class = dict()
        for _class, nb in hotness_rdd.map(lambda x: (self.get_class(x[1]), 1)).reduceByKey(add).collect():
            self.elements_per_class[_class] = nb

    def sample(self, hotness_rdd, desired_el_per_class):
        """
        :return: an rdd (id, (hotness, class))
        """
        desired_el_per_class = float(desired_el_per_class)
        return hotness_rdd \
            .map(lambda x: (self.get_class(x[1]), x)) \
            .sampleByKey(False,
                         {
                             i: 1 if self.elements_per_class[i] <= desired_el_per_class
                             else desired_el_per_class / float(self.elements_per_class[i])
                             for i in self.elements_per_class.keys()
                         }) \
            .map(lambda x: (x[1][0], (x[1][1], x[0])))


def equal_class_sampling_on_names(nb_per_class):
    sc = SparkContext(appName='CorrelationOfData')
    metadata_songs_rdd = get_rdd('metadata-songs', sc)
    # DATA_DIR = 'data/'
    # metadata_songs_rdd = sc.pickleFile(DATA_DIR + 'metadata-songs')
    sampler = EqualClassSampling()
    hotness_rdd = metadata_songs_rdd.map(lambda x: (x[0], x[1]['song_hotttnesss'][0]))\
        .filter(lambda x: analysis_functions.is_numeric(x[1]))\
        .filter(lambda x: float(x[1]) != 0.0)
    title = metadata_songs_rdd.map(lambda x: (x[0], (x[1]['artist_name'][0], x[1]['title'][0])))
    sampler.calculate_nb_elements_per_class(hotness_rdd)
    sampled_hotness_rdd = sampler.sample(hotness_rdd, nb_per_class)
    out = title.join(sampled_hotness_rdd).map(lambda x: (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1])).collect()
    with open(OUT_PATH + 'name_sampling', 'wb') as out_file:
        pickle.dump(out, out_file)


if __name__ == '__main__':
    equal_class_sampling_on_names(int(sys.argv[1]))
