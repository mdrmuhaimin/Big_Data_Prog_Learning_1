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
    if(len(words) > 1):
        yield (words[1], (1, int(words[4])))

def add_occurance_byte(a, b):
    x = a[0] + b[0]
    y = a[1] + b[1]
    return (x, y)

def get_attributes(host_with_attributes):
    x = host_with_attributes[1][0]
    y = host_with_attributes[1][1]
    return ('x', x) , ('y', y), ('xx', x*x), ('yy', y*y), ('xy', x*y), ('n', 1)


def calc_r_vars(a, b):
    return a + b

def output_format(kv):
    k, v = kv
    tuple_output = (k, v[1]/v[0])
    return json.dumps(tuple_output)


text = sc.textFile(inputs)
host_with_trans_data = text.flatMap(get_host_with_trans_data).reduceByKey(add_occurance_byte).flatMap(get_attributes)
# host_with_trans_data.saveAsTextFile(output)
correlation_coefficient_var = host_with_trans_data.reduceByKey(calc_r_vars)
print(type(correlation_coefficient_var))
correlation_coefficient_var.saveAsTextFile(output)


# score_count = sub_reddit_score.reduceByKey(add_occurance_score)
# outdata = score_count.map(output_format)
# outdata.saveAsTextFile(output)
