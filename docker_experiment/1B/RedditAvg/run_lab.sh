export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0
export HADOOP_CLASSPATH=${PWD}/jackson-annotations-2.8.10.jar:${PWD}/jackson-databind-2.8.10.jar:${PWD}/jackson-core-2.8.10.jar

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` RedditAverage.java
${JAVA_HOME}/bin/jar cf redditaverage.jar RedditAverage*.class

wget http://cmpt732.csil.sfu.ca/datasets/reddit-1.zip
unzip reddit-1.zip
rm reddit-1.zip
${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar reddit-1/ output-mapper-regular
${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar reddit-1/ output-reducer-regular
rm -r reddit-1

echo "Mapper output \n"
#cat output-mapper-regular/part-m-00000
echo "\nReducer outout \n"
cat output-reducer-regular/part-r-00000
rm -r output-*

# wget http://cmpt732.csil.sfu.ca/datasets/reddit-2.zip
# unzip reddit-2.zip
# rm reddit-2.zip
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar reddit-2/ output-mapper-bigger
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar reddit-2/ output-reducer-bigger
# rm -r reddit-2

# echo "Mapper output \n"
# #cat output-mapper-bigger/part-m-00000
# echo "\nReducer outout \n"
# cat output-reducer-bigger/part-r-00000
# rm -r output-*

rm -r *.class
rm -r redditaverage.jar
