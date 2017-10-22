#!/usr/bin/env bash
rm -rf output-*
rm -rf wordcount-*

wget http://cmpt732.csil.sfu.ca/datasets/wordcount-2.zip
wget http://cmpt732.csil.sfu.ca/datasets/wordcount-1.zip
unzip wordcount-2.zip
unzip wordcount-1.zip

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WordCountImproved.java
${JAVA_HOME}/bin/jar cf wordcountimproved.jar WordCount*.class

${HADOOP_HOME}/bin/yarn jar wordcountimproved.jar WordCountImproved wordcount-1 output-1
${HADOOP_HOME}/bin/yarn jar wordcountimproved.jar WordCountImproved wordcount-2 output-2

head output-1/part-r-00000
head output-2/part-r-00000

cat output-1/part-* | tail -n 15


