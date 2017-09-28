#!/usr/bin/env bash
rm -r output-*
curl http://cmpt732.csil.sfu.ca/datasets/wordcount-1.zip -o wordcount-1.zip
unzip wordcount-1.zip
rm wordcount-1.zip
spark-submit wordcount-improved.py wordcount-1 output-1
rm -r wordcount-1
cat output-1/part-00000
grep -i "^better" output-1/part*
