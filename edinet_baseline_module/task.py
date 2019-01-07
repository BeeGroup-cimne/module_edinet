import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from module_edinet.module_python2 import BeeModule2
from datetime_functions import date_n_month
from hive_functions import create_hive_module_input_table, create_hive_table_from_hbase_table
from hive_functions.query_builder import RawQueryBuilder
import sys

from module_edinet.edinet_baseline.align_job import MRJob_align


class BaselineModule(BeeModule2):
    """
    Calculate Baselines of edinet buildings
    1. Read the mongo documents in buildings
    2. Read the hive table of the hourly energy consumption"
    3. Read the meteo table and join with the consumprion table
    3. Launch mapreduce job to calculate the baselines with the used data
    """
    def __init__(self):
        super(BaselineModule, self).__init__("edinet_baseline")
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
            args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_baseline/mrjob.conf',
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
            data_companyId_toJoin = params['data_companyId']
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
                    #model = str(item['baseline']['model']) if 'baseline' in item and 'model' in item[
                    #    'baseline'] else 'Weekly30Min'
                    #if str(dev['deviceId'].encode('utf-8')) in device_key.keys():
                    #    device_key[str(dev['deviceId'].encode('utf-8'))].append(
                    #        str(item['modellingUnitId']) + '~' + str(item['devices']) + '~' + model)
                    #else:
                    #    device_key[str(dev['deviceId'].encode('utf-8'))] = [
                    #        str(item['modellingUnitId']) + '~' + str(item['devices']) + '~' + model]

        self.logger.info('A mongo query process has loaded {} devices'.format(len(device_key.keys())))

         ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA FOR MRJOB """
        ######################################################################################################################################################################################


        fields = [('deviceId', 'string'), ('ts', 'int'), ('value', 'float'),
                  ('energyType', 'string'), ('temperature', 'string'), ('stationId', 'string')]

        location = self.config['paths']['measures']

        input_table = create_hive_module_input_table(self.hive, 'edinet_baseline_input',
                                                     location, fields, self.task_UUID)

        #add input table to be deleted after execution
        self.context.add_clean_hive_tables(input_table)
        qbr = RawQueryBuilder(self.hive)
        # select * from (select a.ts, a.deviceid, a.value, a.energytype, a.source, a.data_type, 'C6' as stationid from edinet_hourly_consumption a) a join edinet_meteo b on a.stationid==b.stationid and a.ts==b.ts;
        sentence = """
            INSERT OVERWRITE TABLE {input_table}
            SELECT a.deviceId, a.ts, a.value, a.energyType, b.temperature FROM
            {inner_query}"""
        text = []
        for index, device in enumerate(stations):
            var = "a{}".format(index)
            text.append(""" SELECT {var}.deviceid as deviceId, {var}.ts as ts, {var}.value as value, 
                                   {var}.energyType as energyType, '{station}' as stationid FROM edinet_hourly_consumption {var}
                              WHERE
                                  {var}.deviceid == '{device}'
                              """.format(var=var, device=device, station=stations[device]))
        query_text = """UNION ALL""".join(text)

        join_text = """({query}) a join edinet_meteo b on a.stationid==b.stationid and a.ts==b.ts"""

        vars = {
            'input_table': input_table,
            'inner_query': join_text
        }

        self.logger.debug(sentence.format(**vars))
        qbr.execute_query(sentence.format(**vars))
        ######################################################################################################################################################################################
        """ SETUP MAP REDUCE JOB """
        ######################################################################################################################################################################################

        self.logger.info('Getting')
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('MRJob Align')
            self.launcher_hadoop_job('align', location, companyId, device_key, stations)
        except Exception as e:
             raise Exception('MRJob ALIGN process job has failed: {}'.format(e))
        self.logger.info('Module EDINET_baseline execution finished...')

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = BaselineModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_baseline.task import BaselineModule
from datetime import datetime
params = {
   'companyId': 1092915978,
   'companyId_toJoin': [3230658933],
   'type': 'electricityConsumption',
   'ts_to': datetime(2016, 12, 31, 23, 59, 59)
}
t = BaselineModule()
t.run(params) 
    """