#!/usr/bin/env bash
rm -r output*
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 tpch_orders_cassandra.py mmuhaimi output-cassandra 1079878 627655 220224 1164610
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 tpch_orders_sql.py mmuhaimi output-sql 1079878 627655 220224 1164610
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 tpch_orders_denorm.py mmuhaimi output-denormalize 1079878 627655 220224 1164610
spark-submit --packages anguenot:pyspark-cassandra:0.6.0 tpch_denormalize.py mmuhaimi mmuhaimi