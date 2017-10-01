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
        w = w.lower().strip(' ')
        yield (w, 1)

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordFiltered = words.filter(lambda x:x[0] != '')
wordcount = wordFiltered.reduceByKey(operator.add)

outdataRDD = wordcount.sortBy(lambda x:x[0], True).cache()
outdataByWord = outdataRDD.map(output_format)
outdataByFreq = outdataRDD.sortBy(lambda x:x[1], False).map(output_format)

outdataByWord.saveAsTextFile(output+'/by-word')
outdataByFreq.saveAsTextFile(output+'/by-freq')
