#!/usr/bin/env bash
rm -r output-*
curl http://cmpt732.csil.sfu.ca/datasets/reddit-2.zip -o reddit-2.zip
unzip reddit-2.zip
rm reddit-2.zip
spark-submit relative-score.py reddit-2 output-1
rm -r reddit-2
cat output-1/part-*
