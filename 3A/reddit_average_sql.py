import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit_avg_sql').getOrCreate()
# sc = spark.sparkContext
assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+

inputs = sys.argv[1]
output = sys.argv[2]


def main():
    comments = spark.read.json(inputs)
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

