import sys
from pyspark import SparkConf
import pyspark_cassandra
from pyspark.sql import SparkSession


keyspace = sys.argv[1]
output = sys.argv[2]
orderkeys = sys.argv[3:]

# cluster_seeds = ['127.0.0.1']
cluster_seeds = ['199.60.17.171', '199.60.17.188']

conf = SparkConf().setAppName('TPCH') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
        .set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size, row_format=pyspark_cassandra.RowFormat.DICT) \
        .select('orderkey', 'totalprice', 'part_names')\
        .where('orderkey IN ?', orderkeys)\
        .setName(table)
    return rdd

def main():
    orders_parts = rdd_for(keyspace, 'orders_parts')
    orders_parts = orders_parts.map(lambda row: 'Order #{} ${}:{}'.format(row['orderkey'], round(row['totalprice'], 2), ', '.join(row['part_names'])))
    orders_parts.saveAsTextFile(output)

if __name__ == "__main__":
    main()