from cluster_utils import get_rdd
#import pandas as pd
from pyspark import SparkContext
from pyspark import RDD

def transform_names(string):
    string=str(string)
    string=string.lower()
    #string=remove_accents(string)
    out=""
    for i in string:
        if i.isalpha():
            out= "".join([out,i])
    return out

def get_title_rdd(sc):
    rdd=get_rdd('musicbrainz-songs',sc)
    return rdd.map(lambda x: (x[0],x[1][0][9]))
def get_hit_rdd(sc):
    return sc.pickleFile('~/ADA_Group/project/new_pickle_hit')

def get_summer_hit_rdd(sc):
    id_title=get_title_rdd(sc)
    #id,title
    tra=id_title.map(lambda x:(transform_names(x[1]),x[0]))
    #transformed title,id
    hit_rdd=get_hit_rdd(sc)
    #rank,title(already transformed)
    hit_rdd=hit_rdd.map(lambda x:(x[1],x[0]))
    
    return 
                    #id,rank

if __name__ == '__main__':
    sc = SparkContext()
    rdd =  get_summer_hit_rdd(sc)
    rdd.saveAsPickleFile('hdfs:///user/prevel/hits_key_rank')
            
