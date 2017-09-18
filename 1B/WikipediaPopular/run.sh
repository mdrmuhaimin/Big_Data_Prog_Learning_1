${HADOOP_PREFIX}/bin/hdfs dfs -rm -r pagecounts*
${HADOOP_PREFIX}/bin/hdfs dfs -rm -r output*
${HADOOP_PREFIX}/bin/hdfs dfs -mkdir pagecounts-1/
${HADOOP_PREFIX}/bin/hdfs dfs -copyFromLocal /1B/pagecounts-1/* pagecounts-1/
rm -r *.class
rm -r *.jar
${JAVA_HOME}/bin/javac -classpath `${HADOOP_PREFIX}/bin/hadoop classpath` Runner.java
${JAVA_HOME}/bin/jar cf runner.jar Runner.class
${HADOOP_PREFIX}/bin/hadoop jar runner.jar Runner pagecounts-1/ output-1
${HADOOP_PREFIX}/bin/hdfs dfs -cat output-1/part-r-00000
