from pyspark import SparkContext
from cluster_utils import get_rdd

if __name__ == "__main__":
    sc = SparkContext(appName="Extract years")
    mbz_rdd = get_rdd('musicbrainz-songs', sc)
    mapped_rdd = mbz_rdd.map(lambda x: (x[0], x[1]['year'][0]))
    mapped_rdd.saveAsPickleFile('hdfs:///user/weiskopf/mbz_year')

