from cluster_utils import get_rdd
import pandas as pd

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

def get_summer_hit_rdd(sc):
    rdd=get_title_rdd(sc)
    path_to_csv='../data/hits_clean.csv'
    df=pd.read_csv(path,sep=';')
    for i in df.index:
        df.Title[i]=transform_names(df.Title[i])
    fil=joined.filter(lambda x:(transform_names(x[1]) in df.Title.values))
    fil=fil.map(lambda x:(x[0],df[df.Title==transform_names(x[1])].Rank))
    return fil.map(lambda x:(x[0],x[1].iloc[0]))

if __name__ == '__main__':
    sc = SparkContext()
    rdd =  get_summer_hit_rdd(sc)
    rdd.saveAsPickleFile('hdfs:///user/adams/hits_key_rank')
            
