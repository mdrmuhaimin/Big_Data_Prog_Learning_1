from pyspark import SparkConf, SparkContext
import sys
import json

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('reddit averages')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+


def get_subreddit_data(line):
    reddit_json = json.loads(line)
    yield (reddit_json['subreddit'], reddit_json)


def add_occurance_score(a, b):
    occur_a, sub_reddit_a = a
    occur_b, sub_reddit_b = b
    combined_occurance = occur_a + occur_b
    combined_score = sub_reddit_a + sub_reddit_b
    return (combined_occurance, combined_score)


def calc_avg(kv):
    k, v = kv
    return (k, v[1]/v[0])

def get_relative_score(commendata):
    yield (commendata[1][0]['author'], commendata[1][0]['score'] / commendata[1][1])


text = sc.textFile(inputs)
sub_reddit_datas = text.flatMap(get_subreddit_data).cache()
sub_reddit_score = sub_reddit_datas.map(lambda x: (x[0], (1, x[1]['score'])))
score_count = sub_reddit_score.reduceByKey(add_occurance_score)
reddit_avg = score_count.map(calc_avg)
redditdata_with_avgscore = sub_reddit_datas.join(reddit_avg)
redditdata_with_avgscore = redditdata_with_avgscore.filter(lambda x: x[1][1] > 0)
relative_score = redditdata_with_avgscore.flatMap(get_relative_score).sortBy(lambda x:x[1], False)
relative_score.saveAsTextFile(output)
