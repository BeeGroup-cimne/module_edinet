#! /bin/bash
echo "starting edinet modules"
echo "cleaning hourly data"
python3 module_edinet/launcher_edinet.py clean_hourly
echo "cleaning meteo data"
python3 module_edinet/launcher_edinet.py clean_meteo
echo "calculating hourly baselines"
python3 module_edinet/launcher_edinet.py hourly_baseline
echo "finishing edinet modules"
