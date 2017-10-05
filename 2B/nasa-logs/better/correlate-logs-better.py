from pyspark import SparkConf, SparkContext
import sys
import json
import re
import math

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
    return (x, y, x*x, y*y, x*y, 1)


def calc_r_vars(a, b):
    return tuple(sum(p) for p in zip(a, b))

def add_bytes_and_counts (a, b):
    hit_count = a[0] + b[0]
    bytes_count = a[1] + b[1]
    return (hit_count, bytes_count)

def calc_r_vars_list ( xi, yi, x_avg, y_avg) :
    return (( xi - x_avg ) * ( yi - y_avg ), ( xi - x_avg )*( xi - x_avg ), ( yi - y_avg ) * ( yi - y_avg ))

def calc_r_vars(a, b):
    return tuple(sum(p) for p in zip(a, b))


text = sc.textFile(inputs)
host_with_trans_data = text.flatMap(get_host_with_trans_data)\
    .reduceByKey(add_occurance_byte).cache()

total_host = host_with_trans_data.count()
(total_host_hit, total_byte) = host_with_trans_data.map(lambda x: x[1]).reduce(add_bytes_and_counts)
x_avg = total_host_hit / total_host
y_avg = total_byte / total_host

correlation_coefficient_variables_list = host_with_trans_data.map(lambda x: calc_r_vars_list(x[1][0], x[1][1], x_avg, y_avg))
correlation_coefficient_variables = correlation_coefficient_variables_list.reduce(calc_r_vars)
r = correlation_coefficient_variables[0] / math.sqrt(correlation_coefficient_variables[1] * correlation_coefficient_variables[2])
print(total_host)
print(total_byte)
print(total_host_hit)
print( x_avg, y_avg)
print(r)
print(r*r)
# correlation_coefficient_variables_list.saveAsTextFile(output)
