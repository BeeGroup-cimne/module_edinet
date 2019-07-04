import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from module_edinet.module_python3 import BeeModule3
from hive_functions.query_builder import RawQueryBuilder
from datetime_functions import date_n_month
from hive_functions import create_hive_module_input_table
import sys


class ComparisonModule(BeeModule3):
    def __init__(self):
        super(ComparisonModule, self).__init__("edinet_comparison_module")
        #delete hdfs directory found in config path on finish
#        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    # def launcher_hadoop_job(self, type, input, company=None, devices=None, stations=None, map_tasks=8, red_tasks=8):
    #     """Runs the Hadoop job uploading task configuration"""
    #     # create report to save on completion or error
    #     report = {
    #         'started_at': datetime.now(),
    #         'state': 'launched',
    #         'input': input
    #     }
    #
    #     # Create temporary file to upload with json extension to identify it in HDFS
    #     job_extra_config = self.config.copy()
    #     job_extra_config.update({'devices': devices, 'company': company, 'stations': stations, 'task_id': self.task_UUID})
    #     f = NamedTemporaryFile(delete=False, suffix='.json')
    #     f.write(json.dumps(job_extra_config))
    #     f.close()
    #     report['config_temp_file'] = f.name
    #     self.logger.debug('Created temporary config file to upload into hadoop and read from job: %s' % f.name)
    #     # create hadoop job instance adding file location to be uploaded
    #     # add the -c configuration file
    #     mr_job = MRJob_align(
    #         args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_baseline/mrjob.conf',
    #               '--jobconf', 'mapred.map.tasks=%s' % map_tasks, '--jobconf', 'mapred.reduce.tasks=15'])
    #     # mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani', '--python-archive', path.dirname(lib.__file__)])  # debugger
    #     with mr_job.make_runner() as runner:
    #         try:
    #             runner.run()
    #         except Exception as e:
    #             f.unlink(f.name)
    #             raise Exception('Error running MRJob process using hadoop: {}'.format(e))
    #
    #     f.unlink(f.name)
    #     self.logger.debug('Temporary config file uploaded has been deleted from FileSystem')
    #
    #     report['finished_at'] = datetime.now()
    #     report['state'] = 'finished'
    #
    #     return report


    def module_task(self, params):
        self.logger.info('Starting Module for edinet comparisons ...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            result_companyId = params['result_companyId']
            ts_to = params['ts_to']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -48)
            energyTypeDict = params['type'] if 'type' in params else {
                'gasConsumption': ['heatConsumption', 'gasConsumption'],
                'electricityConsumption': ['electricityConsumption', 'monthlyElectricityConsumption']
            }
            # criteria = sorted([sorted(criteria_set.split(" + ")) for criteria_set in params['criteria']])
            # similar_users_group_calculation = params[
            #     'similar_users_group_calculation'] if 'similar_users_group_calculation' in params else True
            # min_consumption_percentile = params[
            #     'min_consumption_percentile'] if 'min_consumption_percentile' in params else 5
            # max_consumption_percentile = params[
            #     'max_consumption_percentile'] if 'max_consumption_percentile' in params else 95
            # n_months_for_best_criteria_calc = self.config['settings']['similar_users_groups'][
            #     'n_months_for_best_criteria']
            # fixed_percentage_for_cost = self.config['settings']['similar_users_groups'][
            #     'fixed_percentage_for_cost']
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        #####################################################################################################################################################################################
        """  LOAD from MONGO  """
        ######################################################################################################################################################################################
        # Get the link between the devices and the modelling units. In the form of a dict {"device":{modelling_unit~{device:multiplier}}
        self.logger.info('Extracting data from mongodb')
        modelling_units_collection = self.config['mongodb']['modelling_units_collection']
        cursor = self.mongo[modelling_units_collection].find({})
        device_key = {}
        for item in cursor :
            if len(item['devices']) > 0:  # to avoid empty list of devices
                for dev in item['devices']:
                    key_str = "{modelling}~{devices}".format(
                        modelling=item['modellingUnitId'],
                        devices=item['devices'],
                    )
                    if dev['deviceId'] in device_key.keys():
                        device_key[dev['deviceId']].append(key_str)
                    else:
                        device_key[dev['deviceId']] = [key_str]
        cursor.close()

        self.logger.info('A mongo query process has loaded {} devices'.format(len(device_key.keys())))

        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA THAT HAS TO BE LOADED INTO MONGO """
        ######################################################################################################################################################################################

        # create a table with the devices values that will be the input of the MRJob that creates the monthly datatable.
        self.logger.debug('creating input table to aggregate monthly')
        final_table_fields = [[x[0], x[1]] for x in self.config['hive']['final_table_fields']]

        location = self.config['paths']['monthly_aggregation']

        input_table = create_hive_module_input_table(self.hive, self.config['hive']['job_table_name'],
                                                     location, final_table_fields, self.task_UUID)

        #add input table to be deleted after execution
 #       self.context.add_clean_hive_tables(input_table)
        self.logger.debug('creating hive query')
        qbr = RawQueryBuilder(self.hive)

        self.logger.debug("fda")
        total_select_joint = ", ".join(["{}.{}".format(x[2],x[0]) for x in self.config['hive']['final_table_fields']])
        sentence = """
            INSERT OVERWRITE TABLE {input_table}
            SELECT {total_select_joint} FROM
                (SELECT ai.deviceid as deviceId, ai.ts as ts, ai.value as value, ai.energyType as energyType FROM edinet_daily_consumption ai
                    WHERE
                        ai.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
                        ai.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss") AND
                        ai.deviceid IN ({devices})) a
                """.format(input_table=input_table, total_select_joint=total_select_joint, ts_from=ts_from, ts_to=ts_to,
                           devices=", ".join("\"{}\"".format(x) for x in list(device_key.keys())))
        self.logger.debug(sentence)
        qbr.execute_query(sentence)

        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA THAT HAS TO BE LOADED INTO MONGO """
        ######################################################################################################################################################################################

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ComparisonModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_comparison_module.task import ComparisonModule
from datetime import datetime
params = {'result_companyId': 1092915978,'ts_to': datetime(2018, 4, 30, 23, 59, 59)}
t = ComparisonModule()
t.run(params) 
    """