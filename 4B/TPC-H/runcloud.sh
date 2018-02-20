#!/usr/bin/env bash
module load spark
export PYSPARK_PYTHON=python3
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 tpch_orders_sql.py tpch customer
