from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
import datetime
import sys
import re
import math

inputs = sys.argv[1]
output = sys.argv[2]

spark = SparkSession.builder.appName('ingest_logs').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def get_host_with_trans_data(line):
    """Get requesting host, the datetime, the path, and the number of bytes in a request from input as a generator

    @param: line from server log

    """
    Logs = Row("host", "datetime", "path", "bytes")
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    words = line_re.split(line)
    if(len(words) > 1):
        yield (Logs(words[1], datetime.datetime.strptime(words[2], '%d/%b/%Y:%H:%M:%S'), words[3], float(words[4])))


def main():
    text = sc.textFile(inputs)
    host_with_trans_data = text.flatMap(get_host_with_trans_data)
    df = spark.createDataFrame(host_with_trans_data)
    df.write.format('parquet').save(output)

    logs = spark.read.parquet(output)
    logs.createOrReplaceTempView("logs")
    sum_by_host = spark.sql("""
            SELECT host, SUM(bytes)
            FROM logs
            GROUP BY host
        """)

    sum = spark.sql("""
            SELECT SUM(bytes)
            FROM logs
        """)

    sum_by_host.show()
    sum.show()

if __name__ == "__main__":
    main()
