#!/usr/bin/env bash
curl http://cmpt732.csil.sfu.ca/datasets/nasa-logs-1.zip -o nasa-logs-1.zip
unzip nasa-logs-1.zip
rm nasa-logs-1.zip

gzip nasa-logs-1/*

python3 load_logs.py nasa-logs-1 mmuhaimi nasalogs