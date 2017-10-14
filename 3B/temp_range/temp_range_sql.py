import sys
from pyspark.sql import SparkSession, types, functions
import os

inputs = sys.argv[1]
output = sys.argv[2]

spark = SparkSession.builder.appName('temp_range').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

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
    temp = df.createOrReplaceTempView('temp')
    temp_max = spark.sql("""
        SELECT * FROM temp
        WHERE element = 'TMAX' and qflag IS Null""")
    temp_min = spark.sql("""
        SELECT * FROM temp
        WHERE element = 'TMIN' and qflag IS Null""")
    temp_max.createOrReplaceTempView('temp_max')
    temp_min.createOrReplaceTempView('temp_min')
    all_temp_range = spark.sql("""
        SELECT mx.station, mx.date, mx.value1 AS tmax, mn.value1 AS tmin, (mx.value1 - mn.value1) as range 
        FROM temp_max mx
        INNER JOIN temp_min mn
        ON mx.station = mn.station
        AND mx.date = mn.date
    """)
    all_temp_range.createOrReplaceTempView('all_temp_range')

    max_temp_range = spark.sql("""
        SELECT date, MAX(range) as range FROM all_temp_range GROUP BY date
    """)
    max_temp_range.createOrReplaceTempView('max_temp_range')
    complete_max_table = spark.sql("""
        SELECT mx.date, a.station, mx.range
        FROM max_temp_range mx
        INNER JOIN all_temp_range a
        ON mx.date = a.date
        AND mx.range = a.range
        ORDER BY mx.date ASC
    """)
    if not os.path.exists(output):
        os.makedirs(output)
    complete_max_table.write.csv(output, sep=' ', mode='overwrite')

if __name__ == "__main__":
    main()
