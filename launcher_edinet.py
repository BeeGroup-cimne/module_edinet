from datetime import datetime
date_to = datetime.utcnow()
try:
	from module_edinet.tasks import edinet_metering_measures_etl
	params = {}
	x = edinet_metering_measures_etl.delay(params)
except Exception as e:
	print(e)

try:
	from module_edinet.tasks import edinet_billing_measures_etl
	params = {}
	x = edinet_billing_measures_etl.delay(params)
except Exception as e:
	print(e)


try:
	from module_edinet.tasks import edinet_clean_hourly_data_etl
	params = {
                "result_companyId": "1092915978",
                "data_companyId": ["1092915978", "3230658933", "5052736858", "7104124143", "8801761586"],
                "ts_to": date_to
        }
	x = edinet_clean_hourly_data_etl.delay(params)
except Exception as e:
        print(e)
