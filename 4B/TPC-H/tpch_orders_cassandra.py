# from pyspark import SparkConf, SparkContext
import sys
# import re
# import math
from pyspark import SparkConf
import pyspark_cassandra
from pyspark.sql import SparkSession, types, functions
from pyspark.sql import SQLContext


keyspace = sys.argv[1]
output = sys.argv[2]
orderkeys = sys.argv[3:]

cluster_seeds = ['127.0.0.1']
# cluster_seeds = ['199.60.17.171', '199.60.17.188']

conf = SparkConf().setAppName('TPCH') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
        .set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def get_orders_parts(keyspace, split_size=None):
    sqlContext = SQLContext(sc)
    df = sqlContext.\
        createDataFrame(
        sc.cassandraTable(keyspace, 'lineitem', split_size=split_size).
            select('orderkey', 'partkey').
            joinWithCassandraTable(keyspace, 'orders').
            on('orderkey', 'totalprice').
            select('partkey').
            joinWithCassandraTable(keyspace, 'part').
            on('partkey').
            select('name').
            setName('orders_parts')
    )
    df.createOrReplaceTempView('orders')
    return df

def main():
    orders_parts = get_orders_parts(keyspace)
    orders_parts.show()

if __name__ == "__main__":
    main()