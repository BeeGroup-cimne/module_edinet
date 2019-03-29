import cPickle
import os
from datetime import datetime
from tempfile import NamedTemporaryFile
from datetime_functions import date_n_month
from bson import json_util, BSON
import json

from hive_functions.query_builder import RawQueryBuilder
from module_edinet.module_python2 import BeeModule2
import sys
from hive_functions import create_hive_module_input_table, create_hive_table_from_hbase_table
from module_edinet.edinet_clean_meteo_data_etl.hadoop_meteo_job import MRJob_clean_meteo_data

class ETL_clean_meteo(BeeModule2):
    """
    ETL to clean hourly data, detect gaps, outliers and overlappings

    """
    def __init__(self):
        super(ETL_clean_meteo, self).__init__("edinet_clean_meteo_data_etl")

    def launcher_hadoop_job(self, data_type, input, output, result_companyId,  map_tasks=8, red_tasks=8):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({'companyId': result_companyId})
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: %s' % f.name)
        # create hadoop job instance adding file location to be uploaded
        if data_type == "meteo":
                mr_job = MRJob_clean_meteo_data(
                    args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_clean_meteo_data_etl/mrjob.conf',
                        '--output-dir', 'hdfs://' + output,
                        '--jobconf', 'mapred.job.name=edinet_clean_meteo_data_etl', '--jobconf', 'mapred.reduce.tasks=32', '--jobconf', 'mapred.map.tasks=32'])
        else:
            raise Exception("The job with data type {} can not be treated".format(data_type))
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

        self.logger.info('Starting Hbase-HBase ETL using Hadoop to clean meteo data...')

        """CHECK INCONSISTENCIES IN params"""
        try:
            result_companyId = params['result_companyId']
            ts_to = params['ts_to']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -24)
        except KeyError as e:
            raise Exception('Mandatory Parameter not provided: {}'.format(e))

        ######################################################################################################################################################################################
        """ HIVE QUERY TO GET HBASE DATA """
        ######################################################################################################################################################################################
        for measure_config in self.config['measures']:
            # Create temp tables with hbase data, add them to context_clean to be deleted after execution
            table_name = measure_config["hbase_table"]
            self.logger.info('creating {} tables'.format(table_name))
            try:
                keys = measure_config['hbase_keys']
                columns = measure_config['hbase_columns']
                table = create_hive_table_from_hbase_table(self.hive, table_name, table_name, keys, columns, self.task_UUID)
                self.context.add_clean_hive_tables(table)
                self.logger.debug("Created table: {}".format(table))
            except Exception as e:
                self.logger.debug("Error creating table: {}".format(e))
                raise Exception(e)

            fields = measure_config["hive_fields"]

            location = measure_config['measures'].format(UUID=self.task_UUID)
            self.context.add_clean_hdfs_file(location)
            input_table = create_hive_module_input_table(self.hive, measure_config['temp_input_table'],
                                                     location, fields, self.task_UUID)

            #add input table to be deleted after execution
            self.context.add_clean_hive_tables(input_table)
            qbr = RawQueryBuilder(self.hive)
            select = ", ".join([f[0] for f in measure_config["sql_sentence_select"]])
            sentence = """
                INSERT OVERWRITE TABLE {input_table}
                SELECT {select} FROM
                ( """.format(select=select, input_table=input_table)
            var = 'a'
            select = ", ".join([f[1] for f in measure_config["sql_sentence_select"]]).format(var=var)
            where = measure_config["sql_where_select"].format(var=var, ts_from=ts_from, ts_to=ts_to)
            text = """ SELECT {select} FROM {tab} {var}
                       WHERE
                            {where}""".format(var=var, select=select, tab=table,
                                             where=where)
            sentence += text
            sentence += """) unionResult """

            self.logger.debug(sentence)
            qbr.execute_query(sentence)


        ######################################################################################################################################################################################
        """ SETUP MAP REDUCE JOB """
        ######################################################################################################################################################################################
        output_fields = self.config['output']['fields']
        clean_tables = []
        for measure_config in self.config['measures']:
            clean_file_name = measure_config['clean_output_file'].format(UUID=self.task_UUID)
            self.context.add_clean_hdfs_file(clean_file_name)
            clean_table_name = measure_config['clean_output_table']
            self.logger.debug('Launching MR job to clean the daily data')
            try:
                # Launch MapReduce job
                self.launcher_hadoop_job(measure_config['type'], measure_config['measures'].format(UUID=self.task_UUID), clean_file_name, result_companyId)
            except Exception as e:
                raise Exception('MRJob process has failed: {}'.format(e))

            clean_table = create_hive_module_input_table(self.hive, clean_table_name,
                                                         clean_file_name, output_fields, self.task_UUID)
            self.context.add_clean_hive_tables(clean_table)
            clean_tables.append([clean_table, measure_config['type']])
            self.logger.debug("MRJob finished for {}".format(measure_config['type']))

        ######################################################################################################################################################################################
        """ Join the output in a hive table """
        ######################################################################################################################################################################################

        output_file_name = self.config['output']['output_file_name']
        output_hive_name = self.config['output']['output_hive_table']
        output_hive_table = create_hive_module_input_table(self.hive, output_hive_name,
                                                           output_file_name, output_fields)
        try:
            for i in self.hdfs.delete([output_file_name], recurse=True):
                try:
                    i
                except:
                    pass
        except:
            pass
        select = ", ".join([f[0] for f in self.config['output']["sql_sentence_select"]])
        sentence = """
                        INSERT OVERWRITE TABLE {output_table}
                        SELECT {select} FROM
                        ( """.format(select=select, output_table=output_hive_table)
        letter = ''.join(chr(ord('a') + i) for i in range(len(clean_tables) + 1))
        text = []
        for index, tab in enumerate(clean_tables):
            var = letter[index]
            select = ", ".join([f[1] for f in self.config['output']["sql_sentence_select"]]).format(var=var, data_type=tab[1])
            text.append(""" SELECT {select} FROM {tab} {var}
                              """.format(var=var, select=select, tab=tab[0]))
        sentence += """UNION
                    """.join(text)
        sentence += """) unionResult """

        self.logger.debug(sentence)
        qbr = RawQueryBuilder(self.hive)
        qbr.execute_query(sentence)


        self.logger.info('MHbase-HBase ETL clean billing data execution finished...')




if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ETL_clean_meteo()
    job.run(commandDictionary)


    """
from module_edinet.edinet_clean_meteo_data_etl.task import ETL_clean_meteo
from datetime import datetime
params = {
    "result_companyId": "1092915978",
    "ts_to": datetime(2018,12,01)
}
t = ETL_clean_meteo()
t.run(params) 
    """