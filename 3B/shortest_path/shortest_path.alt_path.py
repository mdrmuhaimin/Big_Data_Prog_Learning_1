from pyspark import SparkConf, SparkContext
import sys
import json

inputs = sys.argv[1]
output = sys.argv[2]
start_node = sys.argv[3]
dest_node = sys.argv[4]
file_name = 'links-simple-sorted.txt';

conf = SparkConf().setAppName('shortest path')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def create_source_path_list(line):
    nodes = line.replace(':', '').split(' ')
    source = nodes.pop(0)
    return (source, nodes)

def get_distance_nodes(start_dist, destinations):
    for dest in destinations:
        yield ((dest, (start_dist[0], start_dist[1] + 1)))

def get_smallest_distance(node, distance_tuples):
    if(distance_tuples[1] is None):
        return (node, distance_tuples[0])
    if(distance_tuples[1][0] + distance_tuples[1][1] > distance_tuples[0][0] + distance_tuples[0][1]):
        return (node, distance_tuples[0])
    return (node, distance_tuples[1])

def is_dest_reached(node_element):
    if(node_element[0] == dest_node):
        yield True

def expand_path(path):
    node_link = path[1]
    for node in node_link[1]:
        joined_path = node_link[0] + [node]
        yield (node, joined_path)

def main():
    paths = sc.textFile(inputs+'/'+file_name)
    source = int(start_node)
    sub_reddit_datas = paths.map(create_source_path_list).cache()
    print(sub_reddit_datas.collect())
    path_to_dest = sc.parallelize([(start_node, [start_node])])
    intermediate_nodes = sc.parallelize([(start_node, (source, 0))])
    finalized_nodes = intermediate_nodes
    for i in range(6):
        print(i)
        intermediate_nodes = intermediate_nodes.join(sub_reddit_datas)
        path_to_dest = path_to_dest.join(sub_reddit_datas)
        path_to_dest = path_to_dest.flatMap(expand_path)
        print(path_to_dest.collect())
        intermediate_nodes = intermediate_nodes.flatMap(lambda x: get_distance_nodes(x[1][0], x[1][1]))
        # print(intermediate_nodes.collect())
        finalized_nodes = intermediate_nodes.leftOuterJoin(finalized_nodes).map(lambda x:get_smallest_distance(x[0],x[1]))
        print('Final_join')
        print(finalized_nodes.collect())
        is_reached = finalized_nodes.flatMap(is_dest_reached)
        if(len(is_reached.collect()) > 0):
            break

if __name__ == "__main__":
    main()
