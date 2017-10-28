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

def expand_path(path):
    node_link = path[1]
    for node in node_link[1]:
        joined_path = node_link[0] + [node]
        yield (node, joined_path)

def get_shortest_path(source, current, previous):
    if (previous is None):
        yield (source, current)
    elif(len(current) < len(previous)):
        yield (source, current)

def get_shortest_list(path1, path2):
    if (path1 is None):
        return path2
    if (path2 is None):
        return path1
    if (len(path1) < len(path2)):
        return path1
    return path2

def remove_revisitied_nodes(node):
    if (len(node[1]) > 0):
        yield node

def main():
    paths = sc.textFile(inputs+'/'+file_name)
    sub_reddit_datas = paths.map(create_source_path_list).cache()
    path_to_dest = sc.parallelize([(start_node, [start_node])])
    node_path = sc.parallelize([(start_node, [start_node])])
    found_path_for_node = sc.parallelize([(start_node, [start_node])]).map(lambda x:x[0])
    sc.parallelize(['Node {}: Distance from source = {}, Path from source: {}'.format(start_node, 0, start_node)])\
        .saveAsTextFile(output+ '/iter-0')
    for i in range(6):
        path_to_dest = path_to_dest.join(sub_reddit_datas)
        path_to_dest = path_to_dest.flatMap(expand_path)
        node_path = path_to_dest.leftOuterJoin(node_path).flatMap(lambda x:(get_shortest_path(x[0], x[1][0],x[1][1]))).cache()
        revisited_node = node_path.map(lambda x: x[0]).intersection(found_path_for_node)

        if not revisited_node.isEmpty():
            revisited_node = revisited_node.map(lambda x:(x, []))
            node_path_with_revisited_node = revisited_node.union(node_path)
            node_path_with_flagged_revisited_node = node_path_with_revisited_node.reduceByKey(get_shortest_list)
            node_path_without_revisited_node = node_path_with_flagged_revisited_node.flatMap(remove_revisitied_nodes)
            node_path.unpersist()
            node_path = node_path_without_revisited_node.cache()

        found_path_for_node = found_path_for_node.union(node_path.map(lambda x:x[0])) # adding newly found node_path with already found path
        node_path.map( lambda x:'Node {}: Distance from source = {}, Path from source: {}'.format(x[0], len(x[1]) - 1, '->'.join(x[1])) )\
            .saveAsTextFile(output+ '/iter-' + str(i+1))
        result = node_path.filter(lambda x: x[0] == dest_node)
        if not result.isEmpty():
            result.flatMap( lambda x: x[1]).saveAsTextFile(output+ '/path')
            # sc.coalesce(result.flatMap( lambda x: x[1]))
            node_path.unpersist()
            break

        # Unpersist cached node value and passing it to the next loop as a regular RDD
        temp_final_path = node_path
        node_path.unpersist()
        node_path = temp_final_path
        temp_final_path.unpersist()

if __name__ == "__main__":
    main()
