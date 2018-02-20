#!/usr/bin/env bash

rm -r output-*

curl http://cmpt732.csil.sfu.ca/datasets/graph-1.zip -o graph-1.zip
unzip graph-1.zip
rm graph-1.zip
spark-submit shortest_path.py graph-1 output-1 1 3
rm -r graph-1

echo "         **********          "
cat output-1/*/*





#curl http://cmpt732.csil.sfu.ca/datasets/graph-2.zip -o graph-2.zip
#unzip graph-2.zip
#rm graph-2.zip
#spark-submit shortest_path.py graph-2 output-2 53 76
#rm -r graph-2
#
#echo "         **********          "
#cat output-2/*/*
#
#
#
#
#
#curl http://cmpt732.csil.sfu.ca/datasets/graph-3.zip -o graph-3.zip
#unzip graph-3.zip
#rm graph-3.zip
#spark-submit shortest_path.py graph-3 output-3 3671 210435
#rm -r graph-3
#
#echo "         **********          "
#cat output-3/*/*





#curl http://cmpt732.csil.sfu.ca/datasets/graph-3.zip -o graph-4.zip
#unzip graph-4.zip
#rm graph-4.zip
#spark-submit shortest_path.py graph-4 output-4
#rm -r graph-4
#
#echo "         **********          "
#cat output-4/*/*





echo "         **********          "
