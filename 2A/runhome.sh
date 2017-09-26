rm -r output-*
curl http://cmpt732.csil.sfu.ca/datasets/wordcount-1.zip -o wordcount-1.zip
unzip wordcount-1.zip
rm wordcount-1.zip
spark-submit wordcount.py wordcount-1 output-1
rm -r wordcount-*
