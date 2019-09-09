from datetime import datetime
import glob
import numpy as np
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol
import json
from pymongo import MongoClient
import pandas as pd

class MRJob_benchmarking(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol

    def mapper_init(self):
        fn = glob.glob('*.json')
        self.config = json.load(open(fn[0]))
        self.energyTypeDict = self.config['energyTypeDict']

    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = json.load(open(fn[0]))
        self.company = self.config['company']

    def mapper(self, _, doc):
        # emits comparition criteria as key
        # emits values
        ret = doc.split('\t')
        criterias = self.config['settings']['comparation_criteria']
        input_fields = {name[0]: index for index, name in enumerate(self.config['hive']['benchmarking_table_fields'])}
        for criteria in criterias:
            field = criteria.split("+")
            criteria_key = "+".join([ret[input_fields[f]] for f in field])
            criteria_etype = self.energyTypeDict[ret[input_fields['energyType']]] if ret[input_fields['energyType']] in self.energyTypeDict.keys() else ret[input_fields['energyType']]
            timestamp = datetime.fromtimestamp(float(ret[input_fields['ts']]))
            timestamp_m = timestamp.strftime('%Y%m')
            key = "{}~{}~{}".format(criteria_key, criteria_etype, criteria)
            value = {"value": ret[input_fields['value']], "timestamp": timestamp_m}
            yield key, value

    def reducer(self, key, values):
        # obtain the needed info from the key
        criteria_values, energy_type, criteria = key.split('~')
        df = pd.DataFrame.from_records(values)
        df.value = pd.to_numeric(df.value, errors='coerce')
        breaks = self.config['settings']['breaks']
        final_results = []
        for month, data in df.groupby("timestamp"):
            if len(data) < int(self.config['settings']['min_elements_benchmarking']):
                continue
            comparation_results = {"quantile_{}".format(b): np.nanpercentile(data.value, b) for b in breaks}
            comparation_results.update({"month": month, "num_buildings": len(data)})
            final_results.append(comparation_results)

        #fem la comparació global, amb la mitjana dels ultims 12 mesos per cada mes
        global_df = pd.DataFrame.from_records(final_results)
        global_df = global_df.set_index("month")
        global_df = global_df.sort_index()
        for b in breaks:
            global_df['anual_quantile_{}'.format(b)] = global_df['anual_{}'.format(b)].rolling(12, min_periods=1).mean()

        for i, row in global_df.iterrows():
            mongo = MongoClient(self.config['mongodb']['host'], self.config['mongodb']['port'])
            mongo[self.config['mongodb']['db']].authenticate(
                self.config['mongodb']['username'],
                self.config['mongodb']['password']
            )

            mongo[self.config['mongodb']['db']][self.config['mongodb']['benchmarking_collection']].replace_one(
                {
                    "criteria": criteria,
                    "criteria_values": criteria_values,
                    "energyType":energy_type,
                    "month": i
                },
                {
                    "criteria": criteria,
                    "companyId": self.company,
                    "criteria_values": criteria_values,
                    "energyType":energy_type,
                    "month": i,
                    **row.to_dict()
                },
                upsert=True)
            mongo.close()


if __name__ == '__main__':
    MRJob_benchmarking.run()