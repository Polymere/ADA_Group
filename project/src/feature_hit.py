from analysis_terms import map_rdd_tags,count_tags_weight
from pyspark import SparkContext
from pyspark import RDD
from operator import add
from cluster_utils import get_rdd

if __name__ == '__main__':
    sc = SparkContext()
    main_rdd=map_rdd_tags(sc)
    hit_rdd=sc.pickleFile('hdfs:///user/prevel/new_pickle_hit')
<<<<<<< HEAD
    filtered_rdd=main_rdd.filter(lambda x:[x[0] in hit_rdd.keys]
     list_tags_hit=an_tag.count_tags_weight(filtered_rdd)                        
=======
    hit_keys=hit_rdd.keys().collect()
    filtered_rdd=main_rdd.filter(lambda x:x[4] in hit_keys)
    print('\n len(filtered_rdd)')
    list_tags_hit=count_tags_weight(filtered_rdd)                        
>>>>>>> 00357f70ffed78ed718d9eb458df71ca881c4741
    f = open("output_hit_tag", "w")
    f.write("\n".join(map(lambda x: str(x), list_tags_hit)))
    f.close()
