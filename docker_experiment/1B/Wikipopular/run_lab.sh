rm -rf output-*
rm -rf wordcount-*
rm -r *.class
rm -r *.jar
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WikipediaPopular.java
${JAVA_HOME}/bin/jar cf wikipediapopular.jar WikipediaPopular*.class
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-1/ output-0
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular pagecounts-1/ output-1
cat output-0/part-m-00000
cat output-1/part-r-00000
