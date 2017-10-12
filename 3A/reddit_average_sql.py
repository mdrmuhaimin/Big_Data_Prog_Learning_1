import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit_avg_sql').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+

inputs = sys.argv[1]
output = sys.argv[2]


def main():
    # The schema is encoded in a string.
    schemaString = "subreddit score"
    fields = [types.StructField(field_name, types.StringType(), True) for field_name in schemaString.split()]
    schema = types.StructType(fields)
    comments = spark.read.json(inputs, schema)
    comments.createOrReplaceTempView('comments')
    averages = spark.sql("""
        SELECT subreddit, AVG(score)
        FROM comments
        GROUP BY subreddit
    """)

    averages.show()
    averages.write.save(output, format='json', mode='overwrite')

if __name__ == "__main__":
    main()

