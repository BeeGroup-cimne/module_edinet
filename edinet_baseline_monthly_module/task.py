import os
from datetime import datetime
from subprocess import call
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from module_edinet.module_python2 import BeeModule2
from datetime_functions import date_n_month
from hive_functions import create_hive_module_input_table, create_hive_table_from_hbase_table
from hive_functions.query_builder import RawQueryBuilder
import sys
import pandas as pd
from module_edinet.edinet_baseline_monthly_module.align_job import MRJob_align


class BaselineModule(BeeModule2):
    """
    Calculate Baselines of edinet buildings
    1. Read the mongo documents in buildings
    2. Read the hive table of the hourly energy consumption"
    3. Read the meteo table and join with the consumprion table
    3. Launch mapreduce job to calculate the baselines with the used data
    """
    def __init__(self):
        super(BaselineModule, self).__init__("edinet_baseline_monthly_module")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def launcher_hadoop_job(self, type, input, company=None, devices=None, stations=None, map_tasks=8, red_tasks=8):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({'devices': devices, 'company': company, 'stations': stations, 'task_id': self.task_UUID})
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()
        report['config_temp_file'] = f.name
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: %s' % f.name)
        # create hadoop job instance adding file location to be uploaded
        mr_job = MRJob_align(
            args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_baseline_monthly_module/mrjob.conf',
                  '--jobconf', 'mapred.map.tasks=%s' % map_tasks, '--jobconf', 'mapred.reduce.tasks=%s' % red_tasks])
        # mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani', '--python-archive', path.dirname(lib.__file__)])  # debugger
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                f.unlink(f.name)
                raise Exception('Error running MRJob process using hadoop: {}'.format(e))

        f.unlink(f.name)
        self.logger.debug('Temporary config file uploaded has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report


    def module_task(self, params):
        self.logger.info('Starting Module for edinet baseline...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            result_companyId = params['result_companyId']
            ts_to = params['ts_to']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -24)
            energyTypeList = params['type'] if 'type' in params else []
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))


        ######################################################################################################################################################################################
        """ GET DATA FROM MONGO TO MAKE QUERYS """
        ######################################################################################################################################################################################
        if not energyTypeList:
            energyTypeList = list(set([x['type'] for x in self.mongo['readings'].find({},{'type':1})]))

        #####################################################################################################################################################################################
        """  LOAD DATA FROM HIVE  """
        ######################################################################################################################################################################################

        self.logger.info('Extracting data from mongodb')

        # setting variables for readability
        collection = self.config['mongodb']['modelling_units_collection']

        self.logger.debug('Querying for modelling units in MongoDB')
        cursor = self.mongo[collection].find({})

        device_key = {}
        stations = {}
        for item in cursor:
            if len(item['devices']) > 0:  # to avoid empty list of devices
                for dev in item['devices']:
                    stations[str(dev['deviceId'].encode('utf-8'))] = str(
                        item['stationId']) if 'stationId' in item else None
                    if str(dev['deviceId'].encode('utf-8')) in device_key.keys():
                        device_key[str(dev['deviceId'].encode('utf-8'))].append(
                            str(item['modellingUnitId']) + '~' + str(item['devices']))
                    else:
                        device_key[str(dev['deviceId'].encode('utf-8'))] = [
                            str(item['modellingUnitId']) + '~' + str(item['devices'])]

        self.logger.info('A mongo query process has loaded {} devices'.format(len(device_key.keys())))

         ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA FOR MRJOB """
        ######################################################################################################################################################################################
        # create a table to link devices with stations
        device_stations_df = pd.DataFrame(data={"deviceId": stations.keys(), "stationId": stations.values()},
                                         columns=["deviceId", "stationId"])
        f = NamedTemporaryFile(delete=False, suffix='.csv')
        device_stations_df.to_csv(f.name, header=None, index=None)
        f.close()
        call(["hadoop", "fs", "-mkdir", "-p", f.name, self.config['paths']['stations']])
        call(["hadoop", "fs", "-copyFromLocal", f.name, self.config['paths']['stations']])
        f.unlink(f.name)
        device_stations = create_hive_module_input_table(self.hive, 'edinet_device_stations_table',
                                                         self.config['paths']['stations'],
                                                         [('deviceId', 'string'), ('stationId', 'string')],
                                                         self.task_UUID, sep=",")
        self.context.add_clean_hive_tables(device_stations)

        # create a table with the devices values

        fields = [('deviceId', 'string'), ('ts', 'int'), ('value', 'float'),
                  ('energyType', 'string'), ('temperature', 'string')]

        location = self.config['paths']['measures']

        input_table = create_hive_module_input_table(self.hive, 'edinet_baseline_input',
                                                     location, fields, self.task_UUID)

        #add input table to be deleted after execution
        self.context.add_clean_hive_tables(input_table)
        qbr = RawQueryBuilder(self.hive)
        sentence = """
            INSERT OVERWRITE TABLE {input_table}    
            SELECT a.deviceId, a.ts, a.value, a.energyType, c.temp as temperature FROM
                (SELECT ai.deviceid as deviceId, UNIX_TIMESTAMP(TO_DATE(FROM_UNIXTIME(ai.ts)), "yyyy-MM-dd") as ts, ai.value as value, ai.energyType as energyType 
                FROM edinet_daily_consumption ai
                WHERE
                    ai.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
                    ai.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss")) a
                JOIN {device_stations} b on a.deviceId==b.deviceId
                JOIN (SELECT a2.stationid, AVG(a2.temperature) as temp, UNIX_TIMESTAMP(TO_DATE(FROM_UNIXTIME(a2.ts)), "yyyy-MM-dd") as time FROM edinet_meteo a2 GROUP BY TO_DATE(FROM_UNIXTIME(a2.ts)), a2.stationid) c on b.stationId==c.stationId and a.ts==c.time
            """.format(input_table=input_table, ts_from=ts_from, ts_to=ts_to, device_stations=device_stations)

        self.logger.debug(sentence)
        qbr.execute_query(sentence)

        ######################################################################################################################################################################################
        """ SETUP MAP REDUCE JOB """
        ######################################################################################################################################################################################

        self.logger.info('Getting')
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('MRJob Align')
            self.launcher_hadoop_job('align', location, result_companyId, device_key, stations)
        except Exception as e:
             raise Exception('MRJob ALIGN process job has failed: {}'.format(e))
        self.logger.info('Module EDINET_baseline execution finished...')

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = BaselineModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_baseline_monthly_module.task import BaselineModule
from datetime import datetime
params = {
   'result_companyId': 1092915978,
   'type': 'electricityConsumption',
   'ts_to': datetime(2018, 6, 1, 23, 59, 59)
}
t = BaselineModule()
t.run(params) 
    """

