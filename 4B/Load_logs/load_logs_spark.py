from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from pyspark import SparkConf
import pyspark_cassandra
import os, gzip, re, uuid, datetime, sys

inputs = sys.argv[1]
keyspace = sys.argv[2]
table = sys.argv[3]

cluster_seeds = ['127.0.0.1'] #['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('Load Logs Spark') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
# spark = SparkSession.builder.getOrCreate()

def get_host_with_trans_data(line):
    """Get requesting host, the datetime, the path, and the number of bytes in a request from input as a generator

    @param: line from server log

    """
    # Logs = Row("host", "datetime", "path", "bytes")
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    words = line_re.split(line)
    if(len(words) >= 4):
        yield ({'logid':uuid.uuid1(), 'host':words[1], 'datetime': datetime.datetime.strptime(words[2], '%d/%b/%Y:%H:%M:%S'), 'path': words[3], 'bytes': float(words[4])})


def main():
    text = sc.textFile(inputs)
    partition_count = int(text.count() / 200) # As we want 200 data per partition
    text = text.repartition(partition_count)
    host_with_trans_data = text.flatMap(get_host_with_trans_data)
    # print('Size of partitions')
    # print(host_with_trans_data.mapPartitions(lambda it: [sum(1 for _ in it)]).collect())
    # print('Size of partitions')
    # print(host_with_trans_data.mapPartitions(lambda it: [sum(1 for _ in it)]).collect())
    host_with_trans_data.saveToCassandra(keyspace, table, consistency_level=ConsistencyLevel.ONE)
    # print(host_with_trans_data.collect())
    # text.saveToCassandra()

if __name__ == "__main__":
    main()
