import os
from datetime import datetime
from tempfile import NamedTemporaryFile
from bson import json_util
import json

from datetime_functions import date_n_month
from hive_functions import create_hive_table_from_hbase_table, create_hive_module_input_table
from hive_functions.query_builder import RawQueryBuilder
from module_edinet.module_python2 import BeeModule2
from module_edinet.edinet_weather.align_job import MRJob_align
import sys



class WeatherModule(BeeModule2):
    """
       EDINET_WEATHER MODULE
       - Funcionament ETL:
       1. Agafar dades de mongo i carregar a Hbase (ETL.weather_measures)
       2. Query Hive per recollir les noves dades a la nova finestra de temps
       3. Map Reduce que extregui les timeseries de cada estacio, monta un dataframe i interpola els forats i les guardi a mongo


       """
    def __init__(self):
        super(WeatherModule, self).__init__("edinet_baseline")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def launcher_hadoop_job(self, type, input, red_tasks=8):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))
        mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c',
                                   'module_edinet/edinet_weather/mrjob.conf',
                                   '--jobconf', 'mapred.reduce.tasks=%s' % red_tasks])
            # mr_job = MRJob_align(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani', '--python-archive', path.dirname(lib.__file__)])   # debugger
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
            buffer_size = params['buffer_size'] if 'buffer_size' in params else 1000000
            timezone = params['timezone'] if 'timezone' in params else 'Europe/Madrid'
            ts_to = params['ts_to']
            weatherType = params['type']
            ts_from = date_n_month(ts_to, -36)
            specific_stations = params['specific_stations'] if 'specific_stations' in params else None

        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA THAT HAS TO BE LOADED INTO MONGO """
        ######################################################################################################################################################################################

        hive_keys = {"b": "tinyint", "ts": "bigint", "stationId": "string"}
        columns = [("value", "float", "m:v"), ]
        table_from = create_hive_table_from_hbase_table(self.hive, weatherType, weatherType, hive_keys, columns,
                                                        self.task_UUID)
        table_from_ws = 'stations'
        fields = [('stationid', 'string'), ('ts', 'int'), ('value', 'float'), ('latitude', 'string'),
                  ('longitude', 'string'), ('altitude', 'float')]
        location = self.config['paths']['measures']
        input_table = create_hive_module_input_table(self.hive, 'edinet_weather_input', location, fields, self.task_UUID)
        qbr = RawQueryBuilder(self.hive)

        sentence = """
        INSERT OVERWRITE TABLE {input_table}
        SELECT e.key.stationId, e.key.ts, e.value, s.latitude, s.longitude, s.altitude
        FROM {table_from} e JOIN {table_from_ws} s ON (e.key.stationId = s.key.stationId AND s.key.company = '1234509876')
        WHERE
            e.key.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
            e.key.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss")
        """
        vars = {
            'input_table': input_table,
            'table_from': table_from,
            'table_from_ws': table_from_ws,
            'ts_to': ts_to,
            'ts_from': ts_from
        }
        qbr.execute_query(sentence.format(**vars))

        ######################################################################################################################################################################################
        """ SETUP MAP REDUCE JOB """
        ######################################################################################################################################################################################

        self.logger.info('Getting')
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('Loading the buffer to Hbase')
            self.launcher_hadoop_job('align', location)
        except Exception as e:
            raise Exception('MRJob ALIGN process job has failed: {}'.format(e))


if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = WeatherModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_weather.task import WeatherModule
from datetime import datetime
params = {
    'type': 'temperatureAir',
    'ts_to': datetime(2016, 12, 31, 23, 59, 59)
}
t = WeatherModule()
t.run(params) 
    """