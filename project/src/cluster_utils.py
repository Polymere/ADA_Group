import string

RDD_PATH = 'hdfs:///user/adams/'


def get_rdd(name, sc):
    rdd = sc.emptyRDD()
    for letter in string.ascii_uppercase:
        rdd = rdd.union(sc.pickleFile(RDD_PATH + letter + '/' + name))
    return rdd
