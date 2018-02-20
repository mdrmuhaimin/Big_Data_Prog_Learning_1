#!/usr/bin/env bash
#curl http://cmpt732.csil.sfu.ca/datasets/tmax-1.zip -o tmax-1.zip
#unzip tmax-1.zip
#rm tmax-1.zip
spark-submit weather_predict.py tmax-1  2>/dev/null
