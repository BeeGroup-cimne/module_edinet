import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from module_edinet.module_python2 import BeeModule2
from querybuilder import RawQueryBuilder
from datetime_functions import date_n_month
from hive_functions import create_hive_module_input_table, create_measures_temp_table_edinet
import sys

from module_edinet.edinet_baseline.align_job import MRJob_align


class BaselineModule(BeeModule2):
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
        # add the -c configuration file
        mr_job = MRJob_align(
            args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_baseline/mrjob.conf',
                  '--jobconf', 'mapred.map.tasks=%s' % map_tasks, '--jobconf', 'mapred.reduce.tasks=15'])
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
            companyId = params['companyId'] if 'companyId' in params else None
            companyId_toJoin = params['companyId_toJoin'] if 'companyId_toJoin' in params and params[
                'companyId_toJoin'] else []
            buffer_size = params['buffer_size'] if 'buffer_size' in params else 1000000
            timezone = params['timezone'] if 'timezone' in params else 'Europe/Madrid'
            ts_to = params['ts_to']
            energyType = params['type']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -24)
            modellingUnits = params['modellingUnits'] if 'modellingUnits' in params else []
            debug = params['debug'] if 'debug' in params else None
            remove_tables = params['remove_tables'] if 'remove_tables' in params else True
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        #####################################################################################################################################################################################
        """  LOAD from MONGO to HBASE  """
        ######################################################################################################################################################################################

        self.logger.info('Extracting data from mongodb')

        # set query dictionary
        query = {}
        if params['companyId']:
            query = {'companyId': params['companyId']}
        if modellingUnits:
            query['modellingUnitId'] = {'$in': modellingUnits}

        # set projection dictionary (1 means field returned, 0 field wont be returned)
        projection = {
            '_id': 0,
            '_updated': 0,
            '_created': 0
        }

        # setting variables for readability
        collection = self.config['mongodb']['modelling_units_collection']

        self.logger.debug('Querying for modelling units in MongoDB: %s' % query)
        cursor = self.mongo[collection].find(query, projection)

        device_key = {}
        stations = {}
        for item in cursor:
            if len(item['devices']) > 0:  # to avoid empty list of devices
                for dev in item['devices']:
                    stations[str(dev['deviceId'].encode('utf-8'))] = str(
                        item['stationId']) if 'stationId' in item else None
                    model = str(item['baseline']['model']) if 'baseline' in item and 'model' in item[
                        'baseline'] else 'Weekly30Min'
                    if str(dev['deviceId'].encode('utf-8')) in device_key.keys():
                        device_key[str(dev['deviceId'].encode('utf-8'))].append(
                            str(item['modellingUnitId']) + '~' + str(item['devices']) + '~' + model)
                    else:
                        device_key[str(dev['deviceId'].encode('utf-8'))] = [
                            str(item['modellingUnitId']) + '~' + str(item['devices']) + '~' + model]

        self.logger.info('A mongo query process has loaded {} devices'.format(len(device_key.keys())))

        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA THAT HAS TO BE LOADED INTO MONGO """
        ######################################################################################################################################################################################
        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA THAT HAS TO BE LOADED INTO MONGO """
        ######################################################################################################################################################################################
        # In the previous implementation, only energyType or companyId were taken into account to join the different tables.
        # In this proposal, we will join all tables from companyId and energyType to be the input of baseline module.

        # If energyType is made of a single element(not a list), create a list with this element. Moreover, the energy type is also added for each row
        if not isinstance(energyType, list):
            energyType = [energyType]
            # All companyId will be found in company_to_join
        companyId_toJoin.append(companyId)

        tables = []
        energyTypeList = []
        # Create temp tables with hbase data, add them to context_clean to be deleted after execution
        for i in range(len(energyType)):
            for j in range(len(companyId_toJoin)):
                try:
                    temp_table = create_measures_temp_table_edinet(self.hive, energyType[i], companyId_toJoin[j], self.task_UUID)
                    tables.append(temp_table)
                    self.context.add_clean_hive_tables(temp_table)
                    energyTypeList.append(energyType[i])
                except:
                    pass
        self.logger.debug(len(tables))


        fields = [('deviceId', 'string'), ('ts', 'int'), ('value', 'float'), ('accumulated', 'float'),
                  ('energyType', 'string')]

        location = self.config['paths']['measures']
        input_table = create_hive_module_input_table(self.hive, 'edinet_baseline_input',
                                                     location, fields, self.task_UUID)

        #add input table to be deleted after execution
        self.context.add_clean_hive_tables(input_table)
        qbr = RawQueryBuilder(self.hive, self.logger)
        sentence = """
            INSERT OVERWRITE TABLE {input_table}
            SELECT deviceId, ts, value, accumulated, energyType FROM
            ( """
        letter = ''.join(chr(ord('a') + i) for i in range(len(tables) + 1))
        text = []
        for index, tab in enumerate(tables):
            var = letter[index]
            energy_type = energyTypeList[index]
            text.append(""" SELECT {var}.key.deviceId, {var}.key.ts, {var}.value, {var}.accumulated, '{energy_type}' as energyType FROM {tab} {var}
                              WHERE
                                  {var}.key.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
                                  {var}.key.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss") AND
                                  {var}.key.deviceId IN {devices}
                              """.format(var=var, energy_type=energy_type, tab=tab,
                                         ts_from="{ts_from}", ts_to="{ts_to}", devices="{devices}"))
        sentence += """UNION
                    """.join(text)
        sentence += """) unionResult """
        vars = {
            'input_table': input_table,
            'ts_to': ts_to,
            'ts_from': ts_from,
            'devices': tuple(device_key.keys()) if len(device_key.keys()) > 1 else "('" + ",".join(
                device_key.keys()) + "')"
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