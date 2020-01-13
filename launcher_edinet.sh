#! /bin/bash
echo "starting edinet modules"
echo "cleaning hourly data"
#python3 module_edinet/launcher_edinet.py clean_hourly
#echo "cleaning meteo data"
#python3 module_edinet/launcher_edinet.py clean_meteo
#echo "cleaning daily data"
#python3 module_edinet/launcher_edinet.py clean_daily
#echo "calculating hourly baselines"
#python3 module_edinet/launcher_edinet.py hourly_baseline
#echo "calculating monthly baselines"
#python3 module_edinet/launcher_edinet.py monthly_baseline
echo "calculating comparisons"
python3 module_edinet/launcher_edinet.py comparisons
echo "finishing edinet modules"
