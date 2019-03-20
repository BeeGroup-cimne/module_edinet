from datetime import datetime
date_to = datetime.utcnow()
# tasks definitions
from time import sleep
metering_measures, billing_measures, meteo_measures = None, None, None 

type_energy = ['electricityConsumption', 'electricityReadings', 'unknownConsumption', 'unknownReadings', 'waterConsumption', 'tertiaryElectricityConsumption'] 

# UPLOAD DATA FROM MONGO TO HBASE
try:
        from module_edinet.tasks import edinet_metering_measures_etl
        params = {}
        metering_measures = edinet_metering_measures_etl.delay(params)
except Exception as e:
        print(e)
	exit()
try:
        from module_edinet.tasks import edinet_billing_measures_etl
        params = {}
        billing_measures = edinet_billing_measures_etl.delay(params)
except Exception as e:
        print(e)
	exit()
try:
	from module_edinet.tasks import edinet_meteo_input_etl
	params = {}
	meteo_measures = edinet_meteo_input_etl.delay(params)
except Exception as e:
	print(e)
	exit()
# CLEAN DATA

clean_monthly, clean_hourly, clean_meteo = None, None, None

while True:
	if metering_measures.ready() and clean_hourly is None :	
		try:
        		from module_edinet.tasks import edinet_clean_hourly_data_etl
        		params = {
                		"result_companyId": "1092915978",
                		"data_companyId": ["1092915978", "3230658933", "5052736858", "7104124143", "8801761586"],
                		"ts_to": date_to,
				"type": type_energy
        		}
        		clean_hourly = edinet_clean_hourly_data_etl.delay(params)
		except Exception as e:
        		print(e)
			exit()
	if metering_measures.ready() and billing_measures.ready() and clean_monthly is None:	
		try:
			from module_edinet.tasks import edinet_clean_daily_data_etl
			params = {
				"result_companyId": "1092915978",
                		"data_companyId": ["1092915978", "3230658933", "5052736858", "7104124143", "8801761586"],
                		"ts_to": date_to,
				"type": type_energy
        		}
			clean_monthly = edinet_clean_daily_data_etl.delay(params)
		except Exception as e:
			print(e)
			exit()
	if meteo_measures.ready() and clean_meteo is None:
		try:
			from module_edinet.tasks import edinet_clean_meteo_data_etl
			params = {
				"result_companyId": "1092915978",
				"ts_to": date_to
			}
			clean_meteo = edinet_clean_meteo_data_etl.delay(params)
		except Exception as e:
			print(e)
			exit()
	if clean_hourly is not None and clean_monthly is not None and clean_meteo is not None:
		break
	sleep(1)


#ANALYTICS
baseline_month, baseline_hour, comparison = None, None, None
while True:
	if clean_meteo.ready() and clean_hourly.ready() and clean_monthly.ready() and baseline_month is None: 
		try:
			from module_edinet.tasks import edinet_baseline_monthly_module
			params = {
                		"result_companyId": "1092915978",
                		"ts_to": date_to
       			 }
			baseline_month = edinet_baseline_monthly_module.delay(params)
		except Exception as e:
			print(e)
			exit()
	if clean_hourly.ready() and clean_meteo.ready() and baseline_hour is None:
		try:
			from module_edinet.tasks import edinet_baseline_hourly_module
			params = {
                		"result_companyId": "1092915978",
                		"ts_to": date_to
       			 }
			baseline_hour = edinet_baseline_hourly_module.delay(params)
		except Exception as e:
			print(e)
			exit()
	if clean_hourly.ready() and clean_monthly.ready() and clean_meteo.ready() and comparison is None:
		try: 
			from module_edinet.tasks import edinet_comparison_module
			params = {
    				'result_companyId': 1092915978,
    				'ts_to': date_to,
    				'criteria': ['entityId', 'postalCode', 'useType', 'entityId + postalCode', 'entityId + postalCode + useType', 'entityId + useType']
 			}
			comparison = edinet_comparison_module.delay(params)
		except Exception as e:
			print(e)
			exit()

	if baseline_month is not None and baseline_hour is not None and comparison is not None:
		break
	sleep(1)

