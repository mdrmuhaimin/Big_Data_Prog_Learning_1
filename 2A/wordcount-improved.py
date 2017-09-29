from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import unicodedata

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def words_once(line):
    word_separator = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in word_separator.split(line):
        w = unicodedata.normalize('NFD', w)
        w = w.lower()
        yield (w, 1)


def sort_alpha(kv): # (apple, 2); (banana, 4)
    k, v = kv
    return v, -k

def by_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


text = sc.textFile(inputs)
words = text.flatMap(words_once)
words = words.filter(lambda x: x != '')
wordcount = words.reduceByKey(operator.add)

outdataRDD = wordcount.sortBy(lambda x:x[0], True).cache()
outdataByWord = outdataRDD.map(output_format)
outdataByFreq = outdataRDD.sortBy(lambda x:x[1], False).map(output_format)

outdataByWord.saveAsTextFile(output+'/by-word')
outdataByFreq.saveAsTextFile(output+'/by-freq')
