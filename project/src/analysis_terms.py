from pyspark import RDD
from pyspark import SparkContext
from operator import add



def map_rdd_tags():
    genre_rdd=sc.pickleFile("../data/metadata-artist_terms/")
    #
    weight_rdd=sc.pickleFile("../data/metadata-artist_terms_weight/")
    #
    year_rdd=sc.pickleFile("../data/musicbrainz-songs/")
    metadata_rdd=sc.pickleFile('../data/metadata-songs/')

    f=genre_rdd.map(lambda x: (x[0],x[1]))

    year=year_rdd.map(lambda x: (x[0],x[1]['year'][0]))

    hotness=metadata_rdd.map(lambda x: (x[0],x[1]['song_hotttnesss'][0]))
    #
    weight_rdd=sc.pickleFile("../data/metadata-artist_terms_weight/")
    weight=weight_rdd.map(lambda x:(x[0],x[1]))
    f=f.join(weight)
    #
    joined=f.join(year).join(hotness)
    by=joined.map(lambda x: (x[1][0][1],x[1][0][0][0],x[1][0][0][1],x[1][1],x[0])) #year,array tags,array weight, hotness
    return joined,by

def count_tags_weight(rdd):
    s=rdd.flatMap(lambda x: [(x[1][i], x[2][i]) for i in range(len(x[1]))]).reduceByKey(add)
    c=s.collect()
    return sorted(c,key=lambda x:x[1],reverse=True)

 def filter_year(rdd,year):
    return rdd.filter(lambda x:x[0]==year)


def filter_hotness(rdd,hotness):
    return rdd.filter(lambda x : x[3]>=hotness)

def get_terms_by_song(rdd):
    #find minimal number of terms
    n1=[]
    n2=[]
    col=rdd.collect()
    n=100
    for i in range(len(col)):
        n1.append(len(c[i][1]))
        n2.append(len(c[i][2]))
    return n1,n2

def get_n_tags(rdd,n):
    dropped=rdd.filter(lambda x: (len(x[1]))>=n).filter(lambda x:(len(x[2]))>=n)
    cutted=dropped.map(lambda x:(x[0],x[1][:n],x[2][:n],x[3]))
    return cutted


def get_vector(rdd, terms):
    return rdd.map(lambda x:(x[4][i in terms for i in x[1]]))

def get_vector_terms(year=0,hotness=0,n_terms=50):
    rdd=map_rdd_tags()
    if  year~=0 :
        rdd=filter_year(rdd,year)
    if hotness ~=0:
        rdd=filter_hotness(rdd,hotness)
    tags=count_tags_weight(rdd)
    most_freq=d[:n_terms]
    lst=[]
    for i in most_freq:
        lst.append(i[0])
   return lst,get_vector(rdd,lst)
   
