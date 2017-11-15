import sys
from pyspark.sql import SparkSession, Row, SQLContext, functions
from pyspark.sql.functions import split

topic = sys.argv[1]


spark = SparkSession \
    .builder \
    .appName("Read_Stream") \
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def main():
    messages = spark \
        .readStream \
        .format("kafka") \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()

    lines = messages.select(messages['value'].cast('string'))
    lines = lines.withColumn("x", split("value", "\s+")[0].cast('float'))
    lines = lines.withColumn("x_sq", lines["x"] ** 2)
    lines = lines.withColumn("y", split("value", "\s+")[1].cast('float'))
    lines = lines.withColumn("y_sq", lines["y"] ** 2)
    lines = lines.withColumn("xy", lines["x"] * lines["y"])
    lines = lines.withColumn("n", functions.lit(1))

    # agg_sum_x = lines.groupBy().sum('value_x').withColumnRenamed('sum(value_x)', 'value_x')

    agg_sum = lines.groupBy().sum()

    agg_sum = agg_sum.withColumn('beta', ( agg_sum['sum(xy)'] - ( agg_sum['sum(x)'] * agg_sum['sum(y)'] / agg_sum['sum(n)'] ) ) / ( agg_sum['sum(x_sq)'] - ( agg_sum['sum(x)'] * agg_sum['sum(x)'] / agg_sum['sum(n)'] )) )
    agg_sum = agg_sum.withColumn('alpha', ( (agg_sum['sum(y)'] / agg_sum['sum(n)']) - (agg_sum['sum(x)'] * agg_sum['beta'] / agg_sum['sum(n)']) ))

    alpha_beta  = agg_sum.select('alpha', 'beta')

    messages = alpha_beta\
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    messages.awaitTermination(60)

if __name__ == "__main__":
    main()
