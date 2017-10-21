import sys
from pyspark.sql import SparkSession, types, functions
import math, os

inputs = sys.argv[1]
output = sys.argv[2]

spark = SparkSession.builder.appName('temp_range').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def get_access_time(raw_name):
    date= raw_name.split('-')[-2]
    time = raw_name.split('-')[-1]
    return date + '-' + time[:2]

get_access_time_udf = functions.udf(get_access_time, types.StringType())

def main():
    schema = types.StructType([
        types.StructField('lan', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('total_req', types.IntegerType(), True),
        types.StructField('byte_trans', types.DoubleType(), True),
    ])
    pagecount_df = spark.read.csv(inputs, schema, " ").withColumn('time', get_access_time_udf(functions.input_file_name()))
    pagecount_df = pagecount_df.where((pagecount_df.lan == 'en') & (pagecount_df.title != 'Main_Page') & (pagecount_df.title.startswith('Special:') == False))
    max_hit_page_df = pagecount_df.groupBy('time').max('total_req').select('time', functions.col('max(total_req)').alias('total_req'))
    pagecount_max_df = pagecount_df.join(max_hit_page_df, ['time', 'total_req']).select('time', 'title', 'total_req').sort(['time', 'title'], ascending=True)
    #pagecount_max_df.show()

    if not os.path.exists(output):
        os.makedirs(output)
    pagecount_max_df.write.csv(output, sep=',', mode='overwrite')
if __name__ == "__main__":
    main()
