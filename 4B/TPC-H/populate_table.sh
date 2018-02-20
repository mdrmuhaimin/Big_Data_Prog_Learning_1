#!/usr/bin/env bash
curl http://cmpt732.csil.sfu.ca/datasets/tpch-0.zip -o tpch-0.zip
unzip tpch-0.zip
rm tpch-0.zip
spark-submit --packages anguenot/pyspark-cassandra:0.6.0 tpc_ingest.py tpch-0 mmuhaimi