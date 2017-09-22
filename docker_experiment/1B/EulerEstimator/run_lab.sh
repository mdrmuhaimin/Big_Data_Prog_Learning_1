rm -rf output-*
rm -rf wordcount-*
rm -r *.class
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0
${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` EulerEstimator.java
${JAVA_HOME}/bin/jar cf eulerestimator.jar EulerEstimator*.class
wget http://cmpt732.csil.sfu.ca/datasets/euler-1.zip
unzip euler-1.zip
rm euler-1.zip
${HADOOP_HOME}/bin/yarn jar eulerestimator.jar EulerEstimator -D mapreduce.job.reduces=0 euler-1/ output-0
rm -rf euler-1
# wget http://cmpt732.csil.sfu.ca/datasets/euler-2.zip
# unzip euler-2.zip
# rm euler-2.zip
# ${HADOOP_HOME}/bin/yarn jar eulerestimator.jar EulerEstimator -D mapreduce.job.reduces=0 euler-2/ output-0
# rm -rf euler-2

