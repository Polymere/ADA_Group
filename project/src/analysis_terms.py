from pyspark import SparkContext
from pyspark import RDD
from operator import add
from cluster_utils import get_rdd
import pickle


def map_rdd_tags(sc):
    genre_rdd = get_rdd('metadata-artist_terms', sc)
    weight_rdd = get_rdd('metadata-artist_terms_weight', sc)
    year_rdd = get_rdd('musicbrainz-songs', sc)
    metadata_rdd = get_rdd('metadata-songs', sc)
    f = genre_rdd.map(lambda x: (x[0], x[1]))
    year = year_rdd.map(lambda x: (x[0], x[1]['year'][0]))
    hotness = metadata_rdd.map(lambda x: (x[0], x[1]['song_hotttnesss'][0]))
    weight = weight_rdd.map(lambda x: (x[0], x[1]))
    
    f = f.join(weight)

    joined = f.join(year).join(hotness)
    
    joined = joined.map(lambda x: (x[1][0][1], x[1][0][0][0], x[1][0][0][1], x[1][1], x[0])) 
     # year,array tags,array weight, hotness,id
    return joined


def count_tags_weight(rdd):
    #Flattens the rdd and add the weights for each equal tag
    s=rdd.flatMap(lambda x: [(x[1][i], x[2][i]) for i in range(len(x[1]))]).reduceByKey(add)
    c = s.collect()
    print(len(c))
    return sorted(c, key=lambda x: x[1], reverse=True)
    #Returns a sorted list with the most popular tags in first positions
    
def filter_year(rdd, year):
    return rdd.filter(lambda x: x[0] == year)


def filter_hotness(rdd, hotness):
    return rdd.filter(lambda x: x[3] >= hotness)


def get_vector(rdd, terms):
    #create boolean array with true if the song has a tag in terms, false otherwhise
    return rdd.map(lambda x:(x[4],[i in x[1] for i in terms]))

def get_vector_terms(sc, year=0, hotness=0, n_terms=50):
    rdd = map_rdd_tags(sc)
    rdd.cache()
    if year != 0:
        rdd = filter_year(rdd, year)
    if hotness != 0:
        rdd = filter_hotness(rdd, hotness)
    tags = count_tags_weight(rdd)
    most_freq = tags[:n_terms]
    lst = []
    for i in most_freq:
        lst.append(i[0])
    return lst, get_vector(rdd, lst)


if __name__ == '__main__':
    sc = SparkContext()
    lst, rdd = get_vector_terms(sc)
    rdd.saveAsPickleFile('hdfs:///user/adams/tag_features')
    with open('/buffer/mrp_buffer/tag_features_list.pickle', 'wb') as f:
        pickle.dump(lst, f)
