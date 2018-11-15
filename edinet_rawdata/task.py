import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from hive_functions import create_hive_table_from_hbase_table, create_hive_module_input_table
from hive_functions.query_builder import RawQueryBuilder
from module_edinet.module_python2 import BeeModule2
from module_edinet.edinet_rawdata.create_serie_job import MRJob_create_serie
import sys

class RawDataModule(BeeModule2):
    """
        EDINET_RAWDATA MODULE
        - Funcionament ETL:
        1. Agafar dades de mongo i carregar a Hbase (semblant a ETL.mh_hadoop_v2 - load_job)
        2. Query Hive per recollir les noves dades a la nova finestra de temps
        3. Map Reduce que extregui les timeseries, ordeni i guardi a mongo
    """
    def __init__(self):
        super(RawDataModule, self).__init__("edinet_rawdata")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def launcher_hadoop_job(self, type, input, company=None):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({'company': company})
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.fromat(f.name))
        map_tasks = 8
        red_tasks = 8
        # create hadoop job instance adding file location to be uploaded
        mr_job = MRJob_create_serie(args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_rawdata/mrjob.conf' '--jobconf',
                                              'mapred.map.tasks=%s' % map_tasks, '--jobconf',
                                              'mapred.reduce.tasks=%s' % red_tasks])
            # mr_job = MRJob_create_serie(args=['-r', 'hadoop', 'hdfs://'+input, '--file', f.name, '--output-dir', '/tmp/prova_dani'])  # debugger

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
        self.logger.info('Starting Module for edinet rawdata...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            companyId = params['companyId']  # if 'companyId' in params else None
            buffer_size = params['buffer_size'] if 'buffer_size' in params else 1000000
            timezone = params['timezone'] if 'timezone' in params else 'Europe/Madrid'
            ts_to = params['ts_to']
            ts_from = params['ts_from']  # monthback_date(ts_to,12)
            type = params['type'] if 'type' in params else 'electricityConsumption'

        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))
        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE DATA THAT HAS TO BE LOADED INTO MONGO """
        ######################################################################################################################################################################################
        tables = []
        if not isinstance(type, list):
            type = [type]

        for i in range(len(type)):
            table_name = "{}_{}".format(type[i], companyId)
            hive_keys = {"b": "tinyint", "ts": "bigint", "deviceId": "string"}
            columns = [("value", "float", "m:v"), ("accumulated", "float", "m:va")]
            tables.append(create_hive_table_from_hbase_table(self.hive, table_name, table_name, hive_keys, columns, self.task_UUID))

        fields = [('deviceId', 'string'), ('ts', 'int'), ('value', 'float'), ('accumulated', 'float'),
                    ('energyType', 'string')]
        location = self.config['paths']['measures']
        input_table = create_hive_module_input_table(self.hive, 'edinet_rawdata_measures', location, fields,
                                                     self.task_UUID)
        qbr = RawQueryBuilder(self.hive)

        sentence = """
            INSERT OVERWRITE TABLE {input_table}
            SELECT deviceId, ts, value, accumulated, energyType FROM
            ( """
        letter = ''.join(chr(ord('a') + i) for i in range(len(type)))
        text = []
        for index, tab in enumerate(tables):
                list_text = []
                var = letter[index]
                energy_type = type[index]
                text.append( """ SELECT {var}.key.deviceId, {var}.key.ts, {var}.value, {var}.accumulated, '{energy_type}' as energyType FROM {tab} {var}
                              WHERE                       
                                  {var}.key.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
                                  {var}.key.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss") 
                              """.format(var=var, energy_type=energy_type, tab=tab,
                                         ts_from="{ts_from}", ts_to="{ts_to}"))
        sentence += """UNION ALL""".join(text)
        sentence = sentence + """) unionResult """
        vars = {
                'input_table': input_table,
                'ts_to': ts_to,
                'ts_from': ts_from
        }
        qbr.execute_query(sentence.format(**vars))

        ######################################################################################################################################################################################
        """ SETUP MAP REDUCE JOB """
        ######################################################################################################################################################################################

        self.logger.info('Launching create_serie job process')
        try:
            # Launch MapReduce job
            ## Buffered measures to HBase
            self.logger.debug('Loading the buffer to Hbase')
            self.launcher_hadoop_job('create_serie', location, companyId)
        except Exception as e:
            raise Exception('MRJob create_serie process job has failed: {}'.format(e))


if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = RawDataModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_rawdata.task import RawDataModule
from datetime import datetime
params = {
   'companyId': 1092915978,
   'companyId_toJoin': [3230658933],
   'type': 'electricityConsumption',
   'ts_to': datetime(2016, 12, 31, 23, 59, 59)
}
t = RawDataModule()
t.run(params) 
    """