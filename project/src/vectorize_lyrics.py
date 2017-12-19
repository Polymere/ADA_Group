"""
[This file is intended to be submitted as a job to Apache Spark]

This file is a pipelined version of project/notebooks/Lyrics_analysis.ipynb 
for more details, please have a look over there. The final vectorization part
is not present there though.

This file transforms a list of words 
"""

from pyspark import SparkContext
from cluster_utils import get_rdd

# The amount of vector features to extract
# (this is in fact the number of most occuring words and the number of occurences)
LYRICS_VECTOR_SIZE = 500

# This list is taken from the sklearn stop_words package
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/feature_extraction/stop_words.py
# which in turn is taken from the Glasgow Information Retrieval Group 
# http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words
ENGLISH_STOP_WORDS = [
    "a", "about", "above", "across", "after", "afterwards", "again", "against",
    "all", "almost", "alone", "along", "already", "also", "although", "always",
    "am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
    "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
    "around", "as", "at", "back", "be", "became", "because", "become",
    "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
    "below", "beside", "besides", "between", "beyond", "bill", "both",
    "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con",
    "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
    "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
    "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
    "everything", "everywhere", "except", "few", "fifteen", "fifty", "fill",
    "find", "fire", "first", "five", "for", "former", "formerly", "forty",
    "found", "four", "from", "front", "full", "further", "get", "give", "go",
    "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
    "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his",
    "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed",
    "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
    "latterly", "least", "less", "ltd", "made", "many", "may", "me",
    "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
    "move", "much", "must", "my", "myself", "name", "namely", "neither",
    "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
    "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
    "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
    "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps",
    "please", "put", "rather", "re", "same", "see", "seem", "seemed",
    "seeming", "seems", "serious", "several", "she", "should", "show", "side",
    "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
    "something", "sometime", "sometimes", "somewhere", "still", "such",
    "system", "take", "ten", "than", "that", "the", "their", "them",
    "themselves", "then", "thence", "there", "thereafter", "thereby",
    "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
    "third", "this", "those", "though", "three", "through", "throughout",
    "thru", "thus", "to", "together", "too", "top", "toward", "towards",
    "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
    "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
    "whence", "whenever", "where", "whereafter", "whereas", "whereby",
    "wherein", "whereupon", "wherever", "whether", "which", "while", "whither",
    "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
    "within", "without", "would", "yet", "you", "your", "yours", "yourself",
"yourselves"]

# A few additionnal stemmed words that are not captured by the above
stemmed_stop_words = ['onli', 'whi', 'somethi']


#  --------------------------
#       HELPER FUNCTIONS
#  --------------------------

def flattenDicts(ls):
    """This methods merges dictionaries that are present in a list.
    If the same key is present multiple times, the value of the last 
    dictionary to have this key will be kept."""
    if len(ls) > 1:
        acc = ls[0].copy()
        for x in ls[1:]:
            acc.update(x)
        return acc
    else:
        return ls[0]

def aggWordCount(localRes, newElem):
    """This method aggregates a local result (a dictionary of word counts) 
    together with a datapoint (a list of word occurences for a song) and
    adds the occurences of the song to the local result."""
    if len(localRes) == 0:
        res = {}
        for (k, v) in newElem[1]:
            res[k] = v
        return res
    else:
        for (k, v) in newElem[1]:
            if k in localRes:
                localRes[k] += v
            else:
                localRes[k] = v
        return localRes

def combResults(a, b):
    """This method combines 2 local results (dictionaries of word counts)
    together to form a single one."""
    if len(a) == 0:
        return b
    else:
        res = {}
        for k in a:
            res[k] = a[k]
        for k in b:
            if k in res:
                res[k] += b[k]
            else:
                res[k] = b[k]
        return res

def filter_occ_totals(occurences):
    """Removes all words from the total occurences dictionary that are 
    in the stop words list or in the additionnal words list."""
    acc = {}

    for k in occurences:
        if k not in stemmed_stop_words and k not in ENGLISH_STOP_WORDS:
            acc[k] = occurences[k]
    
    return acc

def map_song_to_vector(keys_list):
    """This method transform an RDD element into a vector.
    It uses a list of words `keys_list` which determines which words'
    occurrence get added to the vector. If the word does not have any 
    occurrence in the current song, this script will add an occurrence 
    count of 0."""
    
    # Building a function that is returned to hack around the fact
    # that keys_list used to be a global variable
    def fn(x):
        vector = []

        #   keys_list is used here
        #        vvvvvvvvv
        for k in keys_list:
            found = False
            for w, occ in x[1]["words"]:
                if k == w:
                    vector.append(occ)
                    found = True
                    break;

            if not found:
                vector.append(0)

        return (x[0], vector)
    
    return fn

#  --------------------------
#        MAIN FUNCTIONS
#  --------------------------

def vectorize_lyrics(lyrics_rdd, musicbrainz_rdd):
    """Main function that applies all of the operations to transform two RDDs into a vectorized one."""
    # Map and join
    mbz_rdd = musicbrainz_rdd.map(lambda x: (x[0], {'year': x[1]['year'][0]}))
    rdd = lyrics_rdd.join(mbz_rdd).map(lambda x: (x[0], flattenDicts(x[1])))
    
    # Calculate the totals
    total_occ = rdd.map(lambda x: (x[0], x[1]["words"])).aggregate({}, aggWordCount, combResults)
    filtered_total_occ = filter_occ_totals(total_occ)
    
    # Find most prevalent words
    sorted_keys = sorted(filtered_total_occ, key=filtered_total_occ.get, reverse=True)
    # Get the N=LYRICS_VECTOR_SIZE best
    most_represented_keys = sorted_keys[:LYRICS_VECTOR_SIZE]
    
    # Vectorize the RDD with those words
    vectorized_rdd = rdd.map(map_song_to_vector(most_represented_keys))
    
    # Return the list of words to map it back for predictions as well
    # as the vectorized RDD
    return most_represented_keys, vectorized_rdd


if __name__ == "__main__":
    sc = SparkContext(appName="Vectorize lyrics")
                                  
    mbz_rdd = get_rdd('musicbrainz-songs', sc)
    lyrics_rdd = sc.pickleFile('hdfs:///user/weiskopf/mxm_dataset_all/')
                                  
    lst, rdd = vectorize_lyrics(lyrics_rdd, mbz_rdd)
    rdd.saveAsPickleFile('hdfs:///user/weiskopf/lyrics_features')
    
    with open('/buffer/mrp_buffer/lyrics_features_list.pickle', 'w') as f:
        pickle.dump(lst, f)
