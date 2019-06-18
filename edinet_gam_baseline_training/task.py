from datetime import datetime
from subprocess import call
from tempfile import NamedTemporaryFile
import pandas as pd
from bson import json_util
import json
import os
from datetime_functions import date_n_month
from hive_functions import create_hive_module_input_table
from hive_functions.query_builder import RawQueryBuilder
from module_edinet.module_python3 import BeeModule3
from module_edinet.edinet_gam_baseline_training.align_job import MRJob_align
import sys
from timezonefinder import TimezoneFinder

class ModelTraining(BeeModule3):
    def __init__(self):
        super(ModelTraining, self).__init__("edinet_gam_baseline_training")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def launcher_hadoop_job(self, input, devices, company, save_data_debug):

        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }
        self.logger.debug("generating the config file info")
        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update(
            {'devices': devices, 'company': company, 'save_data_debug':save_data_debug})
        self.logger.debug("writing the config file")
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(bytes(json.dumps(job_extra_config), encoding="utf8"))
        f.close()
        report['config_temp_file'] = f.name
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))
        # create hadoop job instance adding file location to be uploaded
        # add the -c configuration file
        self.logger.debug('Generating mr jop')
        mr_job = MRJob_align(
            args=['-r', 'hadoop', 'hdfs://{}'.format(input), '--file', f.name, '-c', 'module_edinet/edinet_gam_baseline_training/mrjob.conf',
                  '--dir', 'module_edinet/model_functions#model_functions', '--jobconf', 'mapreduce.job.name=edinet_baseline_gam_hourly_module', '--jobconf', 'mapreduce.job.maps=5', '--jobconf', 'mapreduce.job.reduces=16'])
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
        self.logger.info('Starting Module for edinet baseline...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            result_companyId = params['result_companyId']
            ts_to = params['ts_to']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -24)
            energyTypeList = params['type'] if 'type' in params else []
            save_data_debug = True if 'debug' in params and params['debug'] else False
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        #####################################################################################################################################################################################
        """  LOAD DATA FROM MONGO MODELLING UNITS  """
        ######################################################################################################################################################################################

        self.logger.info('Extracting modelling_units from mongo')

        # setting variables for readability
        modelling_units_collection = self.config['mongodb']['modelling_units_collection']
        weather_stations_collection = self.config['mongodb']['weather_stations_collection']
        lon_lat_tz_dict = {}
        tf = TimezoneFinder(in_memory=True)
        self.logger.debug('Querying for weather station info in MongoDB')
        cursor = self.mongo[weather_stations_collection].find({})
        for station in cursor:
            lon_lat_tz_dict[station['stationId']] = {
                "lat": station['latitude'],
                "lon": station['longitude'],
                "tz": tf.timezone_at(lat=station['latitude'], lng=station['longitude'])
            }
        cursor.close()
        tf = None
        device_key = {}
        stations = {}
        solar_station = {}
        self.logger.debug('Querying for modelling unit info in MongoDB')
        cursor = self.mongo[modelling_units_collection].find({})
        for item in cursor :

            if len(item['devices']) > 0:  # to avoid empty list of devices
                for dev in item['devices']:
                    stations[dev['deviceId']] = item['stationId'] if 'stationId' in item else None
                    solar_station[dev['deviceId']] = item['solar_station'] if 'solar_station' in item else None
                    key_str = "{modelling}~{devices}~{lat}~{lon}~{tz}".format(
                        modelling=item['modellingUnitId'],
                        devices=item['devices'],
                        lat=lon_lat_tz_dict[item['stationId']]['lat'],
                        lon=lon_lat_tz_dict[item['stationId']]['lon'],
                        tz=lon_lat_tz_dict[item['stationId']]['tz']
                    )
                    if dev['deviceId'] in device_key.keys():
                        device_key[dev['deviceId']].append(key_str)
                    else:
                        device_key[dev['deviceId']] = [key_str]
        cursor.close()
        self.logger.info('A mongo query process has loaded {} devices'.format(len(device_key.keys())))

        ######################################################################################################################################################################################
        """ CREATE INPUT DATA FROM HIVE TABLES """
        ######################################################################################################################################################################################
        # create a table to link devices with stations
        self.logger.debug('creating weather hive table')

        weather_stations_df = pd.DataFrame(data={"deviceId": list(stations.keys()), "stationId": list(stations.values())},
                                         columns=["deviceId", "stationId"])

        f_station = NamedTemporaryFile(delete=False, suffix='.csv')
        weather_stations_df.to_csv(f_station.name, header=None, index=None)

        call(["hadoop", "fs", "-mkdir", "-p", f_station.name, self.config['paths']['stations']])
        call(["hadoop", "fs", "-copyFromLocal", f_station.name, self.config['paths']['stations']])
        weather_stations = create_hive_module_input_table(self.hive, 'edinet_weather_stations_table',
                                                         self.config['paths']['stations'],
                                                         [('deviceId', 'string'), ('stationId', 'string')],
                                                         self.task_UUID, sep=",")
        self.context.add_clean_hive_tables(weather_stations)

        # create a table to link devices with solar_stations
        self.logger.debug('creating solar hive table')

        solar_stations_df = pd.DataFrame(
            data={"deviceId": list(solar_station.keys()), "stationId": list(solar_station.values())},
            columns=["deviceId", "stationId"])
        f_solar_station = NamedTemporaryFile(delete=False, suffix='.csv')
        solar_stations_df.to_csv(f_solar_station.name, header=None, index=None)

        call(["hadoop", "fs", "-mkdir", "-p", f_solar_station.name, self.config['paths']['solar_stations']])
        call(["hadoop", "fs", "-copyFromLocal", f_solar_station.name, self.config['paths']['solar_stations']])
        solar_stations = create_hive_module_input_table(self.hive, 'edinet_solar_stations_table',
                                                         self.config['paths']['solar_stations'],
                                                         [('deviceId', 'string'), ('stationId', 'string')],
                                                         self.task_UUID, sep=",")
        self.context.add_clean_hive_tables(solar_stations)

        # create a table with the devices values
        self.logger.debug('creating input table')

        final_table_fields = [[x[0], x[1]] for x in self.config['hive']['final_table_fields']]

        location = self.config['paths']['measures']

        input_table = create_hive_module_input_table(self.hive, self.config['hive']['job_table_name'],
                                                     location, final_table_fields, self.task_UUID)

        #add input table to be deleted after execution
        self.context.add_clean_hive_tables(input_table)
        self.logger.debug('creating hive query')

        qbr = RawQueryBuilder(self.hive)
        total_select_joint = ", ".join(["{}.{}".format(x[2],x[0]) for x in self.config['hive']['final_table_fields']])
        sentence = """
            INSERT OVERWRITE TABLE {input_table}
            SELECT {total_select_joint} FROM
                (SELECT ai.deviceid as deviceId, ai.ts as ts, ai.value as value, ai.energyType as energyType FROM edinet_hourly_consumption ai
                    WHERE
                        ai.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
                        ai.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss") AND
                        ai.deviceid IN ({devices})) a
                JOIN {weather_stations} b on a.deviceId==b.deviceId
                JOIN {solar_stations} b1 on a.deviceId==b1.deviceId
                JOIN  edinet_meteo c on b.stationId==c.stationId and SUBSTR(FROM_UNIXTIME(a.ts), 1, 13) == SUBSTR(FROM_UNIXTIME(c.ts), 1, 13)
                JOIN  edinet_meteo d on b1.stationId==d.stationId and SUBSTR(FROM_UNIXTIME(a.ts), 1, 13) == SUBSTR(FROM_UNIXTIME(d.ts), 1, 13)

                """.format(input_table=input_table, total_select_joint=total_select_joint, ts_from=ts_from, ts_to=ts_to,
                           weather_stations=weather_stations, solar_stations=solar_stations, devices=", ".join("\"{}\"".format(x) for x in list(device_key.keys())))

        self.logger.debug(sentence)
        qbr.execute_query(sentence)


        #####################################################################################################################################################################################
        """  LOAD from MONGO to HBASE  """
        ######################################################################################################################################################################################
        self.logger.info('Getting')
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('Baseline Calculation')
            self.launcher_hadoop_job(location, device_key, result_companyId, save_data_debug)
        except Exception as e:
             raise Exception('MRJob ALIGN process job has failed: {}'.format(e))
        self.logger.info('Module EDINET_baseline execution finished...')

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ModelTraining()
    job.run(commandDictionary)


"""
from module_edinet.edinet_gam_baseline_training.task import ModelTraining
from datetime import datetime
params = {'result_companyId': 1092915978,'ts_to': datetime(2019, 4, 1, 23, 59, 59)}
t = ModelTraining()
t.run(params) 
"""
