from pyspark import SparkConf, SparkContext
import sys
import json
import re

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('reddit averages')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def get_host_with_trans_data(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    words = line_re.split(line)
    print(words)
    if(len(words) > 1):
        yield (words[1], 1, words[4])

def add_occurance_score(a, b):
    occur_a, score_a = a
    occur_b, score_b = b
    return (occur_a + occur_b, score_a + score_b)


def output_format(kv):
    k, v = kv
    tuple_output = (k, v[1]/v[0])
    return json.dumps(tuple_output)


text = sc.textFile(inputs)
host_with_trans_data = text.flatMap(get_host_with_trans_data)
host_with_trans_data.saveAsTextFile(output)
print('Hello')
# score_count = sub_reddit_score.reduceByKey(add_occurance_score)
# outdata = score_count.map(output_format)
# outdata.saveAsTextFile(output)
