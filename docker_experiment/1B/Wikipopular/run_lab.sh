export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0

## Compile 

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WikipediaPopular.java
${JAVA_HOME}/bin/jar cf wikipediapopular.jar WikipediaPopular*.class

## Run regular input's output

wget http://cmpt732.csil.sfu.ca/datasets/pagecounts-1.zip
unzip pagecounts-1.zip
rm pagecounts-1.zip

${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-1/ output-regular-mapper
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular pagecounts-1/ output-regular-reducer

echo "Print mapper output \n"
cat output-regular-mapper/part-m-00000
echo "\n Print reducer output \n"
cat output-regular-reducer/part-r-00000

rm -rf pagecounts-1
rm -r output-*

## Run bigger input's output

wget http://cmpt732.csil.sfu.ca/datasets/pagecounts-2.zip
unzip pagecounts-2.zip
rm pagecounts-2.zip

${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-2/ output-bigger-mapper
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular pagecounts-2/ output-bigger-reducer

echo "Print mapper output \n"
cat output-bigger-mapper/part-m-00000
echo "\n Print reducer output \n"
cat output-bigger-reducer/part-r-00000

rm -rf pagecounts-1
rm -r output-*

rm -r *.class
rm -r *.jar

