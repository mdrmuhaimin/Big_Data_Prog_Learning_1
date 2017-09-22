rm -rf output-*
rm -r *.class
rm -r redditaverage.jar

export HADOOP_CLASSPATH=${PWD}/jackson-annotations-2.8.10.jar:${PWD}/jackson-databind-2.8.10.jar:${PWD}/jackson-core-2.8.10.jar

javac -classpath `hadoop classpath` MultiLineJSONInputFormat.java RedditAverage.java
jar cf redditaverage.jar MultiLineJSONInputFormat*.class RedditAverage*.class
yarn jar redditaverage.jar RedditAverage -D mapreduce.job.reduces=0 -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-1/ output-0
yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-1/ output-1
yarn jar redditaverage.jar RedditAverage -libjars ${PWD}/jackson-annotations-2.8.10.jar,${PWD}/jackson-databind-2.8.10.jar,${PWD}/jackson-core-2.8.10.jar redditmulti-2/ output-2
cat output-0/part-m-00000
cat output-1/part-r-00000
cat output-2/part-r-00000
