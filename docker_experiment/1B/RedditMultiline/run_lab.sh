export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0
export HADOOP_CLASSPATH=${PWD}/jackson-annotations-2.8.10.jar:${PWD}/jackson-databind-2.8.10.jar:${PWD}/jackson-core-2.8.10.jar

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` MultiLineJSONInputFormat.java RedditAverage.java
${JAVA_HOME}/bin/jar cf redditaverage.jar MultiLineJSONInputFormat*.class RedditAverage*.class

# wget http://cmpt732.csil.sfu.ca/datasets/redditmulti-1.zip
# unzip redditmulti-1.zip
# rm redditmulti-1.zip
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-1/ output-0
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-1/ output-1
# rm -r redditmulti-1

# cat output-0/part-m-00000
# cat output-1/part-r-00000

wget http://cmpt732.csil.sfu.ca/datasets/redditmulti-2.zip
unzip redditmulti-2.zip
rm redditmulti-2.zip
${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-2/ output-mapper-multi-big
${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-2/ output-reducer-multi-big
rm -r redditmulti-2

echo "Mapper output \n"
cat output-mapper-multi-big/part-m-00000
echo "Reducer output \n\n"
cat output-reducer-multi-big/part-r-00000


# wget http://cmpt732.csil.sfu.ca/datasets/reddit-1.zip
# unzip reddit-1.zip
# rm reddit-1.zip
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar reddit-1/ output-mapper-reguler
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar reddit-1/ output-reducer-reguler
# rm -r reddit-1

# echo "Mapper output \n"
# cat output-mapper-reguler/part-m-00000
# echo "Reducer output \n\n"
# cat output-reducer-reguler/part-r-00000

rm -rf output-*
rm -rf redditmulti-*
rm -r *.class
rm -r redditaverage.jar