from pyspark import SparkConf, SparkContext
import sys
import re
import math

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('reddit averages')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def get_host_with_trans_data(line):
    """Get host with bytes transfered in a request from input as a generator

    @param: line from server log

    """
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    words = line_re.split(line)
    if(len(words) > 1):
        yield (words[1], (1, int(words[4])))

def add_occurance_byte(a, b):
    """Add requests and bytes transferred to each host

    @param: Input from reducers as a tuple which contain request and byte transferred

    """

    x = a[0] + b[0]
    y = a[1] + b[1]
    return (x, y)

def get_attributes(host_with_attributes):
    """Calculate needed attributes for calculating correlation coefficient variables

    @param: Host with the associated count and bytes transferred

    """

    x = host_with_attributes[1][0]
    y = host_with_attributes[1][1]
    return (x, y, x*x, y*y, x*y, 1)


def calc_r_vars(a, b):
    """Calculate sum of the attributes need to calculate r

    @param: Input from reducers attributes for all host as an iterables

    """
    return tuple(sum(p) for p in zip(a, b))

text = sc.textFile(inputs)
host_with_trans_data = text.flatMap(get_host_with_trans_data)\
    .reduceByKey(add_occurance_byte)\
    .map(get_attributes)
correlation_coefficient_variables = host_with_trans_data.reduce(calc_r_vars)
x = correlation_coefficient_variables[0]
y = correlation_coefficient_variables[1]
xx = correlation_coefficient_variables[2]
yy = correlation_coefficient_variables[3]
xy = correlation_coefficient_variables[4]
n = correlation_coefficient_variables[5]

r = (n * xy - x * y) / math.sqrt((n * xx - x * x) * ( n * yy - y * y))
print('r = {}'.format(r))
print('r^2 = {}'.format(r * r))

sc.parallelize(['r={}\nr^2={}'.format(r, r*r)]).saveAsTextFile(output)