${HADOOP_PREFIX}/bin/hdfs dfs -rm -r output*
${HADOOP_PREFIX}/bin/hdfs dfs -rm -r pagecounts*
${HADOOP_PREFIX}/bin/hdfs dfs -mkdir pagecounts-0/
${HADOOP_PREFIX}/bin/hdfs dfs -copyFromLocal /1A/pagecounts-0/* pagecounts-0/
rm -r *.class
rm -r *.jar
${JAVA_HOME}/bin/javac -classpath `${HADOOP_PREFIX}/bin/hadoop classpath` WordCountImproved.java
${JAVA_HOME}/bin/jar cf wordcountimproved.jar WordCount*.class
${HADOOP_PREFIX}/bin/hadoop jar wordcountimproved.jar WordCountImproved pagecounts-0/ output-0
${HADOOP_PREFIX}/bin/hdfs dfs -cat output-0/part-r-00000
