#!/usr/bin/env bash
rm -r output-*
#curl http://cmpt732.csil.sfu.ca/datasets/weather-1.zip -o weather-1.zip
#unzip weather-1.zip
#rm weather-1.zip
spark-submit temp_range_fix.py weather-1 output-1-flag-fix
spark-submit temp_range.py weather-1 output-1-no-flag-fix
#rm -r weather-1

#curl http://cmpt732.csil.sfu.ca/datasets/weather-2.zip -o weather-2.zip
#unzip weather-2.zip
#rm weather-2.zip
#spark-submit temp_range.py weather-2 output-2
#rm -r weather-2

#curl http://cmpt732.csil.sfu.ca/datasets/weather-3.zip -o weather-3.zip
#unzip weather-3.zip
#rm weather-3.zip
#spark-submit temp_range.py weather-3 output-3
#rm -r weather-3
echo "         **********          "
echo "\\nOutput with no flag fix\\n"
cat output-1-no-flag-fix/part-*
echo "         **********          "
echo "\nOutput with flag fix\n"
cat output-1-flag-fix/part-*
#cat output-2/part-*
#cat output-3/part-*
