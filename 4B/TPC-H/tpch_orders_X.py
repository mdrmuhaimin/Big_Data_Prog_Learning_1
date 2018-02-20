from pyspark import SparkConf, SparkContext
import sys
import re
import math
from pyspark import SparkConf
import pyspark_cassandra
import os, gzip, re, uuid, datetime, sys, math
from pyspark.sql import SparkSession, types, functions


keyspace = sys.argv[1]
table = sys.argv[2]

conf = SparkConf().setAppName('reddit averages').set('spark.dynamicAllocation.maxExecutors', 20)
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size,
        row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
    return rdd

def df_for(keyspace, table, split_size=None):
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    df.createOrReplaceTempView(table)
    return df

def main():
    tpch = df_for(keyspace, table)
    print(tpch.collect())

if __name__ == "__main__":
    main()