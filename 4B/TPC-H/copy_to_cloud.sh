#!/usr/bin/env bash
#scp tpch_orders_X.py mmuhaimi@gateway.sfucloud.ca:
scp -r ${PWD}/tpch_orders_* mmuhaimi@gateway.sfucloud.ca:4B/tpch/
scp -r ${PWD}/runcloud.sh mmuhaimi@gateway.sfucloud.ca:4B/tpch/