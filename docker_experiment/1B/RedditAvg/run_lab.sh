rm -rf output-*
rm -rf wordcount-*
rm -r *.class
rm -r *.jar
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` RedditAverage.java
${JAVA_HOME}/bin/jar cf redditaverage.jar RedditAverage*.class
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 pagecounts-1/ output-0
# ${HADOOP_HOME}/bin/yarn jar redditaverage.jar RedditAverage pagecounts-1/ output-1
# cat output-0/part-m-00000
# cat output-1/part-r-00000
