from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
sys.path.insert(0,'.')
# parameters
date_to = datetime.now()
date_from_hourly = date_to - relativedelta(years=2)
date_from_monthly = date_to - relativedelta(years=2)


# tasks definitions

# UPLOAD DATA FROM MONGO TO HBASE
def metering_measures():
	try:
		from module_edinet.tasks import edinet_metering_measures_etl
		params = {}
		metering_measures = edinet_metering_measures_etl.delay(params)
		return metering_measures.wait()
	except Exception as e:
		print(e)
		raise e


def billing_measures():
	try:
		from module_edinet.tasks import edinet_billing_measures_etl
		params = {}
		billing_measures = edinet_billing_measures_etl.delay(params)
		return billing_measures.wait()
	except Exception as e:
		print(e)
		raise e


def meteo_measures():
	print("fiesta")
	try:
		from module_edinet.tasks import edinet_meteo_input_etl
		params = {}
		meteo_measures = edinet_meteo_input_etl.delay(params)
		return meteo_measures.wait()
	except Exception as e:
		print(e)
		raise e


# CLEAN DATA

def clean_hourly():
	try:
		from module_edinet.tasks import edinet_clean_hourly_data_etl
		params = {
			"result_companyId": "1092915978",
			"ts_to": date_to,
			"ts_from": date_from_hourly
		}
		clean_hourly = edinet_clean_hourly_data_etl.delay(params)
		return clean_hourly.wait()
	except Exception as e:
		print(e)
		raise e


def clean_daily():
	try:
		from module_edinet.tasks import edinet_clean_daily_data_etl
		params = {
			"result_companyId": "1092915978",
			"ts_to": date_to,
			"ts_from": date_from_monthly
		}
		clean_monthly = edinet_clean_daily_data_etl.delay(params)
		return clean_monthly.wait()
	except Exception as e:
		print(e)
		raise e


def clean_meteo():
	try:
		from module_edinet.tasks import edinet_clean_meteo_data_etl
		meteo_date = min(date_from_hourly, date_from_monthly)
		params = {
			"result_companyId": "1092915978",
			"ts_to": date_to,
			"ts_from": meteo_date
		}
		clean_meteo = edinet_clean_meteo_data_etl.delay(params)
		return clean_meteo.wait()
	except Exception as e:
		print(e)
		raise e


def monthly_baseline():
	try:
		from module_edinet.tasks import edinet_baseline_monthly_module
		params = {
			"result_companyId": "1092915978",
			"ts_to": date_to
		}
		baseline_month = edinet_baseline_monthly_module.delay(params)
		return baseline_month.wait()
	except Exception as e:
		print(e)
		raise e


def hourly_baseline():
	try:
		from module_edinet.tasks import edinet_baseline_hourly_module
		params = {
			"result_companyId": "1092915978",
			"ts_to": date_to
		}
		baseline_hour = edinet_baseline_hourly_module.delay(params)
		return baseline_hour.wait()
	except Exception as e:
		print(e)
		raise e


def comparisons():
	try:
		from module_edinet.tasks import edinet_comparison_module
		params = {
			'result_companyId': 1092915978,
			'ts_to': date_to,
		}
		comparison = edinet_comparison_module.delay(params)
		return comparison.wait()
	except Exception as e:
		print(e)
		raise e


def train_gam():
	try:
		from module_edinet.tasks import edinet_gam_baseline_training
		params = {
			'result_companyId': 1092915978,
			'ts_to': date_to,
		}
		comparison = edinet_gam_baseline_training.delay(params)
		return comparison.wait()
	except Exception as e:
		print(e)
		raise e

def predict_gam():
	try:
		from module_edinet.tasks import edinet_gam_baseline_prediction
		params = {
			'result_companyId': 1092915978,
			'ts_to': date_to,
		}
		comparison = edinet_gam_baseline_prediction.delay(params)
		return comparison.wait()
	except Exception as e:
		print(e)
		raise e
# try:
# 	from module_edinet.tasks import edinet_gam_baseline_training
# 	params = {
# 				"result_companyId": "1092915978",
# 				"ts_to": date_to
# 		 }
# 	baseline_hour = edinet_gam_baseline_training.delay(params)
# except Exception as e:
# 	print(e)
# 	exit()
#

if __name__ == "__main__":
	module = sys.argv[1]
	globals()[module]()