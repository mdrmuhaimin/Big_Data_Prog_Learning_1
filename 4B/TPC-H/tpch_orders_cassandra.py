# from pyspark import SparkConf, SparkContext
import sys
# import re
# import math
from pyspark import SparkConf
import pyspark_cassandra
from pyspark_cassandra import Row
from pyspark.sql import SparkSession, SQLContext


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
    rdd = sc.cassandraTable(keyspace, 'lineitem', split_size=split_size).\
            select('orderkey', 'partkey').\
            where('orderkey IN ?', orderkeys).setName('lineitem')

    rdd_lineitem_joined_order =  rdd.joinWithCassandraTable(keyspace, 'orders').\
            on('orderkey').\
            select('totalprice'). \
            map(lambda row: Row(orderkey=row[0]['orderkey'], partkey=row[0]['partkey'], totalprice=row[1]['totalprice'])).setName('orders')

    rdd_lineitem_joined_order_partkey = rdd_lineitem_joined_order.joinWithCassandraTable(keyspace, 'part').\
            on('partkey').\
            select('name').\
            map(lambda row: Row(orderkey=row[0]['orderkey'], totalprice=row[0]['totalprice'], name=row[1]['name'])).setName('part')

    df = sqlContext.createDataFrame(rdd_lineitem_joined_order_partkey)
    return df

def main():
    orders_parts = get_orders_parts(keyspace)
    orders_parts.show()

if __name__ == "__main__":
    main()
