${HADOOP_PREFIX}/bin/hdfs dfs -rm -r output*
${HADOOP_PREFIX}/bin/hdfs dfs -mkdir wordcount-1/
${HADOOP_PREFIX}/bin/hdfs dfs -copyFromLocal /1A/wordcount-1/* wordcount-1/
rm -r *.class
rm -r *.jar
${JAVA_HOME}/bin/javac -classpath `${HADOOP_PREFIX}/bin/hadoop classpath` WordCountImproved.java
${JAVA_HOME}/bin/jar cf wordcountimproved.jar WordCount*.class
${HADOOP_PREFIX}/bin/hadoop jar wordcountimproved.jar WordCountImproved wordcount-1/ output-1
${HADOOP_PREFIX}/bin/hdfs dfs -cat output-1/part-r-00000
