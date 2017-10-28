from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import os, gzip, re, uuid, datetime, sys

inputs = sys.argv[1]
namespace = sys.argv[2]
table = sys.argv[3]

def main():
    # cluster = Cluster() # Uncomment for localhost
    cluster = Cluster(['199.60.17.171', '199.60.17.188']) # Uncoment for using it on cluster
    session = cluster.connect(namespace)
    insert_log = session.prepare("INSERT INTO "+table+" (logid, host, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    query_count = 0
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8', errors='ignore') as logfile:
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
                words = line_re.split(line)
                if(len(words) > 1):
                    query_count += 1
                    batch.add(insert_log, (uuid.uuid1(), words[1], datetime.datetime.strptime(words[2], '%d/%b/%Y:%H:%M:%S'), words[3], int(words[4])))
                if(query_count == 200):
                    session.execute(batch)
                    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                    query_count = 0
    session.execute(batch)

if __name__ == "__main__":
    main()
