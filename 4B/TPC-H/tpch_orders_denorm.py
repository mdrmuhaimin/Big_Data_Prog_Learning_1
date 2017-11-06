import sys
from pyspark import SparkConf
from cassandra import ConsistencyLevel
import pyspark_cassandra
from pyspark_cassandra import Row
from pyspark.sql import SparkSession, SQLContext, functions as f, types

input_keyspace = sys.argv[1]
output_keyspace = sys.argv[2]

cluster_seeds = ['127.0.0.1']
# cluster_seeds = ['199.60.17.171', '199.60.17.188']

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

toStringList = f.UserDefinedFunction(lambda names: names.split(' '), types.ArrayType(types.StringType()))

def row_to_dict(row):
    return row.asDict()

def save_rdd_to_cassandra(rdd):
    rdd.saveToCassandra(output_keyspace, 'orders_parts', consistency_level=ConsistencyLevel.ONE, batch_size = 100)

def main():
    orders = df_for(input_keyspace, 'orders')
    parts = df_for(input_keyspace, 'part')
    lines = df_for(input_keyspace, 'lineitem')
    orders = spark.sql("""SELECT o.*, p.name FROM
              orders o
              JOIN lineitem l ON (o.orderkey = l.orderkey)
              JOIN part p ON (l.partkey = p.partkey)
              """)
    order_list = orders.withColumn('part_names', toStringList(orders['name'])).drop('name')
    order_list = order_list.rdd.map(row_to_dict)
    # order_list = sc.parallelize([{'orderkey': 869410, 'clerk': 'Clerk#000000817', 'comment': 'uriously bold dinos after the even ideas boo', 'custkey': 9025, 'order_priority': '1-URGENT', 'orderdate': '1996-02-17', 'orderstatus': 'O', 'ship_priority': 0, 'totalprice': 299817, 'part_names': ['floral', 'honeydew', 'red', 'orchid', 'hot']}])
    save_rdd_to_cassandra(order_list)
    # print(order_list.collect())

if __name__ == "__main__":
    main()