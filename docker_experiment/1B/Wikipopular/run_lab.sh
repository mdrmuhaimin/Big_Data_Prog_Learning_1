export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HADOOP_HOME=/usr/shared/CMPT/big-data/hadoop-2.6.0

## Compile 

${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WikipediaPopular.java
${JAVA_HOME}/bin/jar cf wikipediapopular.jar WikipediaPopular*.class

## Run tiny input's output

wget http://cmpt732.csil.sfu.ca/datasets/pagecounts-0.zip
unzip pagecounts-0.zip
rm pagecounts-0.zip

${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-0/ output-tiny-mapper
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular pagecounts-0/ output-tiny-reducer

rm -rf pagecounts-0


## Run regular input's output

wget http://cmpt732.csil.sfu.ca/datasets/pagecounts-1.zip
unzip pagecounts-1.zip
rm pagecounts-1.zip

${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-1/ output-regular-mapper
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular pagecounts-1/ output-regular-reducer

rm -rf pagecounts-1

## Run bigger input's output

wget http://cmpt732.csil.sfu.ca/datasets/pagecounts-2.zip
unzip pagecounts-2.zip
rm pagecounts-2.zip

${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular -D mapreduce.job.reduces=0 pagecounts-2/ output-bigger-mapper
${HADOOP_HOME}/bin/yarn jar wikipediapopular.jar WikipediaPopular pagecounts-2/ output-bigger-reducer

rm -rf pagecounts-2


# echo "Print tiny input's mapper output \n"
# cat output-tiny-mapper/part-m-00000
echo "\n Print tiny input's reducer output \n"
cat output-tiny-reducer/part-r-00000

# echo "Print regular input's mapper output \n"
# cat output-regular-mapper/part-m-00000
echo "\n Print regular input's reducer output \n"
cat output-regular-reducer/part-r-00000

# echo "Print bigger input's mapper output \n"
# cat output-bigger-mapper/part-m-00000
echo "\n Print bigger input's reducer output \n"
cat output-bigger-reducer/part-r-00000

rm -r output-*

rm -r *.class
rm -r *.jar

