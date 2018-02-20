from cassandra import ConsistencyLevel
from pyspark import SparkConf
import pyspark_cassandra
import re, uuid, datetime, sys

inputs = sys.argv[1]
keyspace = sys.argv[2]
table = sys.argv[3]
NUM_EXECUTOR = 50


# cluster_seeds = ['127.0.0.1'] # Uncomment for using with localhost
cluster_seeds = ['199.60.17.171', '199.60.17.188']

conf = SparkConf().setAppName('Load Logs Spark') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

def get_host_with_trans_data(line):
    """Get requesting host, the datetime, the path, and the number of bytes in a request from input as a generator

    @param: line from server log

    """
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    words = line_re.split(line)
    if(len(words) >= 4):
        yield ({'logid':str(uuid.uuid1()), 'host':words[1], 'datetime': datetime.datetime.strptime(words[2], '%d/%b/%Y:%H:%M:%S'), 'path': words[3], 'bytes': float(words[4])})


def main():
    text = sc.textFile(inputs)
    partition_count = NUM_EXECUTOR * 3
    text = text.repartition(partition_count)
    host_with_trans_data = text.flatMap(get_host_with_trans_data)
    host_with_trans_data.saveToCassandra(keyspace, table, consistency_level=ConsistencyLevel.ONE)

if __name__ == "__main__":
    main()
