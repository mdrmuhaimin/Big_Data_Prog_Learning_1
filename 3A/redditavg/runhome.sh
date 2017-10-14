#!/usr/bin/env bash
rm -r output-*
curl http://cmpt732.csil.sfu.ca/datasets/reddit-1.zip -o reddit-1.zip
unzip reddit-1.zip
rm reddit-1.zip
spark-submit reddit_average_sql.py reddit-1 output-1
rm -r reddit-1
cat output-1/part-*
