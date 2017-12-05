from pyspark import RDD
from pyspark import SparkContext
from operator import add


def map_rdd_tags():
    genre_rdd=sc.pickleFile("../data/metadata-artist_terms/")
    year_rdd=sc.pickleFile("../data/musicbrainz-songs/")
    metadata_rdd=sc.pickleFile('../data/metadata-songs/')

    f=genre_rdd.map(lambda x: (x[0],x[1]))

    year=year_rdd.map(lambda x: (x[0],x[1]['year'][0]))

    hotness=metadata_rdd.map(lambda x: (x[0],x[1]['song_hotttnesss'][0]))
    joined=f.join(year).join(hotness)
    by=joined.map(lambda x: (x[1][0][1],x[1][0][0],x[1][1])) #year,array tags, hotness
    return by

def filter_year(rdd,year):
    return rdd.filter(lambda x:x[0]==year)

def filter_hotness(rdd,hotness):
    return rdd.filter(lambda x : x[2]>=hotness)

def count_tags(rdd):
    m=rdd.flatMap(lambda x: [i for i in x[1]]).map(lambda x : (x,1)).reduceByKey(add)
    c=m.collect()
    return sorted(c,key=lambda x:x[1],reverse=True)

def filter_year_hotness(rdd,year,hotness):
    return filter_year(filter_hotness(rdd,hotness),year)
    
