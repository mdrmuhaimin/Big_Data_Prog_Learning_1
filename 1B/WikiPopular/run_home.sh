#!/usr/bin/env bash

curl http://cmpt732.csil.sfu.ca/datasets/pagecounts-1.zip -o pagecounts-1.zip
unzip pagecounts-1.zip
rm pagecounts-1.zip

rm -r output-*
rm -r *.class
rm -r *.jar
javac -classpath `hadoop classpath` WikipediaPopular.java
jar cf WikipediaPopular.jar WikipediaPopular*.class
yarn jar WikipediaPopular.jar WikipediaPopular pagecounts-1/ output-1
hdfs dfs -cat output-1/*

rm -r pagecounts-1
