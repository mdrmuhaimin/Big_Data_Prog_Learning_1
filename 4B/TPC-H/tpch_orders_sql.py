import sys
from pyspark import SparkConf
import pyspark_cassandra
from pyspark.sql import SQLContext, SparkSession, types, functions


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
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size,
        row_format=pyspark_cassandra.RowFormat.DICT).setName(table)
    return rdd

def df_for(keyspace, table, split_size=None):
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    df.createOrReplaceTempView(table)
    return df

def agg_orders(order_rdd):
    order_rdd.cache()
    orders_name = order_rdd \
        .select('orderkey', 'name') \
        .groupby("orderkey") \
        .agg(functions.collect_set("name").alias('name'))
    orders_price = order_rdd \
        .select('orderkey', 'totalprice') \
        .groupby("orderkey") \
        .agg(functions.sum("totalprice").alias('totalprice'))
    order_rdd.unpersist()
    return orders_name.join(orders_price, 'orderkey')


toStringList = functions.UserDefinedFunction(lambda names:  ', '.join(names), types.StringType())

def main():
    orders = df_for(keyspace, 'orders')
    parts = df_for(keyspace, 'part')
    lines = df_for(keyspace, 'lineitem')
    orders = spark.sql("""SELECT o.orderkey, o.totalprice, p.name FROM
              orders o
              JOIN lineitem l ON (o.orderkey = l.orderkey)
              JOIN part p ON (l.partkey = p.partkey)
              WHERE o.orderkey IN {0}
              """.format(tuple(orderkeys)))
    orders = agg_orders(orders)
    orders = orders.rdd.map(lambda row: 'Order #{} ${}:{}'.format(row.orderkey, round(row.totalprice, 2), ', '.join(row.name)))
    orders.saveAsTextFile(output)

if __name__ == "__main__":
    main()