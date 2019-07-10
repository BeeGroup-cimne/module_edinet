import os
from datetime import datetime
from subprocess import call
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from module_edinet.module_python3 import BeeModule3
from hive_functions.query_builder import RawQueryBuilder
from datetime_functions import date_n_month
from hive_functions import create_hive_module_input_table
import sys
from module_edinet.edinet_comparison_module.aggregate_monthly import MRJob_aggregate
from module_edinet.edinet_comparison_module.benchmarking import MRJob_benchmarking
import pandas as pd
import gc
class ComparisonModule(BeeModule3):
    def __init__(self):
        super(ComparisonModule, self).__init__("edinet_comparison_module")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def aggregate_hadoop_job(self, input, output, devices, company):

        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }
        self.logger.debug("generating the config file info")
        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update(
            {'devices': devices, 'company': company})
        self.logger.debug("writing the config file")
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(bytes(json.dumps(job_extra_config), encoding="utf8"))
        f.close()
        report['config_temp_file'] = f.name
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))
        # create hadoop job instance adding file location to be uploaded
        # add the -c configuration file
        self.logger.debug('Generating mr jop')
        self.logger.debug(output)
        mr_job = MRJob_aggregate(
            args=['-r', 'hadoop', 'hdfs://{}'.format(input), '--file', f.name,
                  '--output-dir', 'hdfs://{}'.format(output), '-c', 'module_edinet/edinet_comparison_module/mrjob.conf',
                  '--jobconf', 'mapreduce.job.name=edinet_comparison'])
        # mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani', '--python-archive', path.dirname(lib.__file__)])  # debugger
        self.logger.debug('running mr job')
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                os.unlink(f.name)
                raise Exception('Error running MRJob process using hadoop: {}'.format(e))

        os.unlink(f.name)
        self.logger.debug('Temporary config file uploaded has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report

    def benchmarking_hadoop_job(self, input, energyTypeDict, company):

        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }
        self.logger.debug("generating the config file info")
        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update(
            {'energyTypeDict': energyTypeDict, 'company': company})
        self.logger.debug("writing the config file")
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(bytes(json.dumps(job_extra_config), encoding="utf8"))
        f.close()
        report['config_temp_file'] = f.name
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))
        # create hadoop job instance adding file location to be uploaded
        # add the -c configuration file
        self.logger.debug('Generating mr jop')
        mr_job = MRJob_benchmarking(
            args=['-r', 'hadoop', 'hdfs://{}'.format(input), '--file', f.name,
                  '-c', 'module_edinet/edinet_comparison_module/mrjob.conf',
                  '--jobconf', 'mapreduce.job.name=edinet_comparison_benchmarking'])
        # mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani', '--python-archive', path.dirname(lib.__file__)])  # debugger
        self.logger.debug('running mr job')
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                os.unlink(f.name)
                raise Exception('Error running MRJob process using hadoop: {}'.format(e))

        os.unlink(f.name)
        self.logger.debug('Temporary config file uploaded has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report

    def module_task(self, params):
        self.logger.info('Starting Module for edinet comparisons ...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            result_companyId = params['result_companyId']
            ts_to = params['ts_to']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -48)
            energyTypeDict = params['type'] if 'type' in params else {'heatConsumption': 'gasConsumption',
                                                                      'gasConsumption': 'gasConsumption',
                                                                      'monthlyElectricityConsumption': 'electricityConsumption',
                                                                      'electricityConsumption': 'electricityConsumption'
                                                                      }
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

        def get_building(modelling_unit, mongo, building_collection, reporting_collection):
            building = mongo[building_collection].find_one({"modellingUnits": modelling_unit})
            if not building:
                reporting = mongo[reporting_collection].find_one({"modelling_Units": modelling_unit})
                if reporting and "reportingUnitId" in reporting:
                    building = mongo[building_collection].find_one({"buildingId": reporting['reportingUnitId']})
            if not building:
                return None
            return building
        building_collection = self.config['mongodb']['buildings_collection']
        reporting_collection = self.config['mongodb']['reporting_collection']
        self.logger.debug("generating the device_key dict")
        for item in cursor:
            #self.logger.debug(item)
            #self.logger.debug("gettinng item building {}".format(item['modellingUnitId']))
            building = get_building(item['modellingUnitId'], self.mongo, building_collection, reporting_collection)
            #self.logger.debug("obtained building {}".format(building))

            if building and 'data' in building and 'areaBuild' in building['data']:
                surface = building["data"]["areaBuild"]
            else:
                surface = None
            #self.logger.debug("area of building: {}".format(surface))
            if len(item['devices']) > 0 and surface:  # to avoid empty list of devices
                self.logger.debug("list of devices {}".format(item['devices']))
                for dev in item['devices']:
                    key_str = "{modelling}~{devices}~{area}".format(
                        modelling=item['modellingUnitId'],
                        devices=item['devices'],
                        area = surface
                    )
                    if dev['deviceId'] in device_key.keys():
                        device_key[dev['deviceId']].append(key_str)
                    else:
                        device_key[dev['deviceId']] = [key_str]
            #self.logger.debug("finished for {}".format(item['modellingUnitId']))
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
        self.context.add_clean_hive_tables(input_table)
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
        self.hive.close()
        self.logger.debug("AAAAAAAAAAAAAAAAAAAAAAAAA")
        gc.collect()
        ######################################################################################################################################################################################
        """ MAPREDUCE TO AGGREGATE MONTHLY DATA """
        ######################################################################################################################################################################################
        self.logger.info('Running Mapreduce for Montly Aggregation')
        output_location = self.config['paths']['output_monthly_aggregation']
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('Montly Aggregation')
            self.aggregate_hadoop_job(location, output_location, device_key, result_companyId)
        except Exception as e:
            raise Exception('MRJob ALIGN process job has failed: {}'.format(e))

        output_fields = [["modellingUnit", "string"], ["ts", "bigint"], ["value", "float"], ["energyType", "string"]]
        aggregated_table_name = self.config['hive']['output_monthly_aggregation']
        aggregated_table = create_hive_module_input_table(self.hive, aggregated_table_name,
                                                          output_location, output_fields, self.task_UUID)
        self.context.add_clean_hive_tables(aggregated_table)
        self.logger.debug("MRJob for monthly aggregation finished")
        ######################################################################################################################################################################################
        """ MAPREDUCE TO CALCULATE BENCHMARKING """
        ######################################################################################################################################################################################
        self.logger.debug('creating benchmarking information table')
        building_collection = self.config['mongodb']['buildings_collection']
        cursor = self.mongo[building_collection].find({})
        buildings_list = []
        for item in cursor :
            if not 'modellingUnits' in item or not 'data' in item:
                continue
            if not 'useType' in item['data'] or not 'organizationLevel1' in item['data']:
                continue
            for modelling in item['modellingUnits']:
                b_dic={"modellingunit":modelling, "type": item['data']['useType'], "organization":item['data']['organizationLevel1']}
                buildings_list.append(b_dic)
        cursor.close()

        buildings_df = pd.DataFrame.from_records(buildings_list, columns=['modellingunit','type','organization'])
        f_station = NamedTemporaryFile(delete=False, suffix='.csv')
        buildings_df.to_csv(f_station.name, header=None, index=None)
        call(["hadoop", "fs", "-mkdir", "-p", f_station.name, self.config['paths']['building_info']])
        call(["hadoop", "fs", "-copyFromLocal", f_station.name, self.config['paths']['building_info']])
        building_table = create_hive_module_input_table(self.hive, self.config['hive']['building_info_table'],
                                                        self.config['paths']['building_info'],
                                                        [('modellingunit', 'string'), ('type', 'string'),('organization','string')],
                                                        self.task_UUID, sep=",")
        self.context.add_clean_hive_tables(building_table)

        self.logger.debug('creating hive query to join data with information')
        qbr = RawQueryBuilder(self.hive)
        location = self.config['paths']['benchmarking_data']
        benchmarking_field = self.config['hive']['benchmarking_table_fields']
        benchmarking_table = create_hive_module_input_table(self.hive, self.config['hive']['benchmarking_table'],
                                                    location, benchmarking_field, self.task_UUID)

        total_select_joint = ", ".join(["{}.{}".format(x[2], x[0]) for x in benchmarking_field])
        sentence = """
           INSERT OVERWRITE TABLE {input_table}
           SELECT {total_select_joint} FROM
               (SELECT * FROM {aggregated_table}) a
               JOIN {building_table} b on a.modellingUnit==b.modellingUnit
               """.format(input_table=benchmarking_table, total_select_joint=total_select_joint,
                          aggregated_table=aggregated_table, building_table=building_table)
        self.logger.debug(sentence)
        qbr.execute_query(sentence)

        self.logger.info('Running Mapreduce for Benchmarking')
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('Benchmarking_calculation')
            self.benchmarking_hadoop_job(location, energyTypeDict, result_companyId)
        except Exception as e:
            raise Exception('MRJob ALIGN process job has failed: {}'.format(e))

        self.logger.debug("MRJob for benchmarking finished")

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ComparisonModule()
    job.run(commandDictionary)

    """
from module_edinet.edinet_comparison_module.task import ComparisonModule
from datetime import datetime
params = {'result_companyId': 1092915978,'ts_to': datetime(2019,7,9)}
t = ComparisonModule()
t.run(params) 
    """