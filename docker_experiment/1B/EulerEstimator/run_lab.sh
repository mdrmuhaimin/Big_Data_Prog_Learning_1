rm -rf output-*
rm -rf wordcount-*
rm -r *.class
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` EulerEstimator.java
${JAVA_HOME}/bin/jar cf eulerestimator.jar EulerEstimator*.class
${HADOOP_HOME}/bin/yarn jar eulerestimator.jar EulerEstimator -D mapreduce.job.reduces=0 euler-1/ output-0
cat output-0/part-m-00000

