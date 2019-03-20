#-*- coding: utf-8 -*-
"""
use migrate_metering to migrate:
   "unknownConsumption_1092915978"
   "unknownReadings_1092915978"
   "electricityConsumption_1092915978"
   "waterConsumption_1092915978"
   "electricityReadings_1092915978"
   "electricityConsumption_5052736858"
   "electricityConsumption_3230658933"
   "electricityConsumption_7104124143"
   "electricityConsumption_8801761586"
   "tertiaryElectricityConsumption_8801761586"

use migrate_csv_inergy to migrate:
  tertiaryElectricityConsumption_3230658933


danger:
 gas consumption ??
 - gasConsumption_1092915978
 - gasConsumption5052736858
 - gasConsumption_3230658933
 - gasConsumption_7104124143
 - gasConsumption_8801761586
 bad tertiarys
 - tertiaryElectricityConsumption_1092915978
 - tertiaryElectricityConsumption_7104124143

"""
import pandas as pd
from datetime import datetime
import requests
class BeedataClient():
    def __init__(self, api_base_url, token, CA_BUNDLE=None):
        self.api_base_url = api_base_url
        self.headers = {"Content-Type": "application/json", "Authorization": "Bearer {}".format(token)}
        self.bundle = CA_BUNDLE if CA_BUNDLE else False
    def get(self, api_endpoint, return_response=False):
        response = requests.get("{}/{}".format(self.api_base_url, api_endpoint), headers=self.headers, verify=self.bundle)
        if not return_response and response.ok:
            return response.json()
        return response

    def post(self, api_endpoint, json, return_response=False):
        response = requests.post("{}/{}".format(self.api_base_url, api_endpoint), json=json, headers=self.headers, verify=self.bundle)
        if not return_response and response.ok:
            return response.json()
        return response

    def patch(self, api_endpoint, json, return_response=False):
        response_get = requests.get("{}/{}".format(self.api_base_url, api_endpoint), headers=self.headers, verify=self.bundle)
        if not response_get.ok:
            raise Exception("Error finding resource {}".format(response_get.content))
        etag = response_get.json()['_etag']
        etag_headers = dict({"If-Match": etag})
        etag_headers.update(self.headers)
        response = requests.patch("{}/{}".format(self.api_base_url, api_endpoint), json=json, headers=etag_headers, verify=self.bundle)
        if not return_response and response.ok:
            return response.json()
        return response

    def delete(self, api_endpoint, return_response=False):
        response_get = requests.get("{}/{}".format(self.api_base_url, api_endpoint), headers=self.headers, verify=self.bundle)
        if not response_get.ok:
            raise Exception("Error finding resource {}".format(response_get.content))
        etag = response_get.json()['_etag']
        etag_headers = dict({"If-Match": etag})
        etag_headers.update(self.headers)
        response = requests.delete("{}/{}".format(self.api_base_url, api_endpoint), headers=etag_headers, verify=self.bundle)
        if not return_response and response.ok:
            return response.json()
        return response


file_path = "/Users/eloigabal/Desktop/edinet/Factures_corrected.xlsx"

beedata_api = "https://api2.edinet.cimne.com/v1"

token = 'VF7NZL35QBMVY4RLEW22MA237A6RPEB9NPMD2FLM7CRNTVWQ9V'

client = BeedataClient(beedata_api, token)

beedata_api_measures = 'billing_amon_measures'

df = pd.read_excel(file_path ,converters={'CUPS': str})


df.DataInici = pd.to_datetime(df.DataInici, format="%Y/%m/%d %H:%M:%S")
df.DataFi = pd.to_datetime(df.DataFi, format="%Y/%m/%d %H:%M:%S")
df.DataInici = df.DataInici.apply(lambda x: datetime.isoformat(x).__add__("Z"))
df.DataFi = df.DataFi.apply(lambda x: datetime.isoformat(x).__add__("Z"))

devices = pd.unique(df.CUPS)
type_energy = "tertiaryElectricityConsumption"

for device in devices:
    df_device = df[df.CUPS == device]
    measurements = {
        "measurements": [
            {
                "timestamp_start_bill": row.DataInici,
                "timestamp_end_bill": row.DataFi,
                "type": type_energy,
                "values": {
                    "p1": row.consumP1,
                    "p2": row.consumP2,
                    "p3": row.consumP3,
                    "p4": row.consumP4,
                    "p5": row.consumP5,
                    "p6": row.consumP6
                }
            }
            for row in df_device.itertuples()
        ],
        "readings": [
            {
                "type": type_energy,
                "unit": "kWh"
            }
        ],
        "deviceId": device
    }
    print(len(measurements['measurements']))
    status = client.post(beedata_api_measures, json=measurements, return_response=True)
    if not status.ok:
        print("{} error processing".format(file_path))

