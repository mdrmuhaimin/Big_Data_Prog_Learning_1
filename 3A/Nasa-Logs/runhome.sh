#!/usr/bin/env bash
rm -r output-*
#curl http://cmpt732.csil.sfu.ca/datasets/nasa-logs-1.zip -o nasa-logs-1.zip
#unzip nasa-logs-1.zip
#rm nasa-logs-1.zip
spark-submit ingest_logs.py nasa-logs-1 output-1
#rm -r nasa-logs-1

#curl http://cmpt732.csil.sfu.ca/datasets/nasa-logs-2.zip -o nasa-logs-2.zip
#unzip nasa-logs-2.zip
#rm nasa-logs-2.zip
#spark-submit ingest_logs.py nasa-logs-2 output-2
#rm -r nasa-logs-2

#cat output-1/part-*
#cat output-2/part-*

#cat output-1/by-freq/part-*
#grep -i "^better" output-1/by-word/part*
#cat output-1/by-word/part-* | tail -n 15
