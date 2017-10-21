import sys
from pyspark.sql import SparkSession, types, functions
import math, os

inputs = sys.argv[1]
output = sys.argv[2]

spark = SparkSession.builder.appName('temp_range').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def get_dif(list):
    diff = len(list)*-1.00
    if(len(list) == 2):
        diff = math.fabs(list[0] - list[1])
    return diff

def main():
    schema = types.StructType([
        types.StructField('station', types.StringType(), True),
        types.StructField('date', types.StringType(), True),
        types.StructField('element', types.StringType(), True),
        types.StructField('value1', types.DoubleType(), True),
        types.StructField('mflag', types.StringType(), True),
        types.StructField('qflag', types.StringType(), True),
    ])
    df = spark.read.csv(inputs, schema)

    getRange = functions.udf(get_dif, types.DoubleType())

    df_by_date = df.select('station', 'date', 'value1')\
        .where((df.element == 'TMAX') | (df.element == 'TMIN'))\
        .groupBy('station', 'date') \
        .agg(functions.collect_list('value1').alias('range'))\
        .withColumn('range', getRange('range'))
    df_by_date = df_by_date.where(df_by_date.range > 1).sort('date', ascending=True)
    df_max = df_by_date.groupBy('date').max('range').select('date', functions.col('max(range)').alias('range'))
    joined_df = df_max.join(df_by_date, ["date", "range"], 'inner')
    joined_df = joined_df.select('date', 'station', 'range')
    joined_df.show()
    if not os.path.exists(output):
        os.makedirs(output)
    joined_df.write.csv(output, sep=' ', mode='overwrite')

if __name__ == "__main__":
    main()
