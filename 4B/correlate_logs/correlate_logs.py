from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from pyspark import SparkConf
import pyspark_cassandra
import os, gzip, re, uuid, datetime, sys, math

keyspace = sys.argv[1]
table = sys.argv[2]

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']

conf = SparkConf().setAppName('Load Logs Spark') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
# spark = SparkSession.builder.getOrCreate()

def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size,
        row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
    return rdd

def get_host_with_trans_data(dictionary):
    return (dictionary['host'], (1, dictionary['bytes']))

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

def main():
    nasalogs = rdd_for(keyspace, table)
    # print('Size of partitions')
    # print(nasalogs.mapPartitions(lambda it: [sum(1 for _ in it)]).collect())
    host_with_trans_data = nasalogs.map(get_host_with_trans_data).reduceByKey(add_occurance_byte).cache()
    total_host = host_with_trans_data.count()
    (total_host_hit, total_byte) = host_with_trans_data.map(lambda x: x[1]).reduce(add_bytes_and_counts)
    x_avg = total_host_hit / total_host
    y_avg = total_byte / total_host

    correlation_coefficient_variables_list = host_with_trans_data.map(
        lambda x: calc_r_vars_list(x[1][0], x[1][1], x_avg, y_avg))
    correlation_coefficient_variables = correlation_coefficient_variables_list.reduce(calc_r_vars)
    print(correlation_coefficient_variables)
    r = correlation_coefficient_variables[0] / math.sqrt(correlation_coefficient_variables[1] * correlation_coefficient_variables[2])

    print('r = {}'.format(r))
    print('r^2 = {}'.format(r * r))

if __name__ == "__main__":
    main()
