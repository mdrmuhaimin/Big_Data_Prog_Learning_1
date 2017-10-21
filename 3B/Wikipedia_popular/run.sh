#!/usr/bin/env bash
rm -r output-*
curl http://cmpt732.csil.sfu.ca/datasets/pagecounts-1.zip -o pagecounts-1.zip
unzip pagecounts-1.zip
rm pagecounts-1.zip
spark-submit wikipedia_popular.py pagecounts-1 output-1
rm -r pagecounts-1

#curl http://cmpt732.csil.sfu.ca/datasets/pagecounts-2.zip -o pagecounts-2.zip
#unzip pagecounts-2.zip
#rm pagecounts-2.zip
#spark-submit wikipedia_popular.py pagecounts-2 output-2
#rm -r pagecounts-2

#curl http://cmpt732.csil.sfu.ca/datasets/pagecounts-3.zip -o pagecounts-3.zip
#unzip pagecounts-3.zip
#rm pagecounts-3.zip
#spark-submit wikipedia_popular.py pagecounts-3 output-3
#rm -r pagecounts-3

echo "         **********          "
cat output-1/part-*
echo "         **********          "
#cat output-2/part-*
#echo "         **********          "
#cat output-3/part-*
#echo "         **********          "
