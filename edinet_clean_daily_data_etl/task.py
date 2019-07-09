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
from module_edinet.edinet_clean_daily_data_etl.hadoop_billing_job import MRJob_clean_billing_data
from module_edinet.edinet_clean_daily_data_etl.hadoop_metering_job import MRJob_clean_metering_data

class ETL_clean_daily(BeeModule2):
    """
    ETL to clean billing data, detect gaps, outliers and overlappings
    1. Get the billing data from HBASE grouped by device and procedence

    """
    def __init__(self):
        super(ETL_clean_daily, self).__init__("edinet_clean_daily_data_etl")

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
        if data_type == "billing":
            mr_job = MRJob_clean_billing_data(
                args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_clean_daily_data_etl/mrjob.conf',
                    '--output-dir', 'hdfs://' + output,
                    '--jobconf', 'mapred.job.name=edinet_clean_daily_data_etl_billing', '--jobconf', 'mapred.reduce.tasks=15', '--jobconf', 'mapred.map.tasks=32'])
        elif data_type == "metering":
                mr_job = MRJob_clean_metering_data(
                    args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_clean_daily_data_etl/mrjob.conf',
                        '--output-dir', 'hdfs://' + output,
                        '--jobconf', 'mapred.job.name=edinet_clean_daily_data_etl_metering', '--jobconf', 'mapred.reduce.tasks=15', '--jobconf', 'mapred.map.tasks=32'])
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

        self.logger.info('Starting Hbase-HBase ETL using Hadoop to clean billing data...')

        """CHECK INCONSISTENCIES IN params"""
        try:
            result_companyId = params['result_companyId']
            data_companyId = params['data_companyId'] if 'data_companyId' in params else []
            ts_to = params['ts_to']
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -96)
            energyTypeList = params['type'] if 'type' in params else []
        except KeyError as e:
            raise Exception('Mandatory Parameter not provided: {}'.format(e))


        ######################################################################################################################################################################################
        """ GET DATA FROM MONGO TO MAKE QUERYS """
        ######################################################################################################################################################################################
        if not energyTypeList:
            energyTypeList = list(set([x['type'] for x in self.mongo['readings'].find({},{'type':1})]))

        if not data_companyId:
            data_companyId = list(set([x['companyId'] for x in self.mongo['companies'].find({},{'companyId':1})]))

        ######################################################################################################################################################################################
        """ HIVE QUERY TO GET HBASE DATA """
        ######################################################################################################################################################################################
        for measure_config in self.config['measures']:
            # Create temp tables with hbase data, add them to context_clean to be deleted after execution
            tables = []
            type_table_name = measure_config["hbase_table"]
            tables_source = []
            tables_energyType = []
            self.logger.info('creating {} tables for {} and {}'.format(type_table_name, energyTypeList, data_companyId))

            tables_list = self.hbase.tables()
            for energyType in energyTypeList:
                for companyId in data_companyId:
                    try:
                        table_name = "{}_{}_{}".format(type_table_name, energyType, companyId)
                        if table_name not in tables_list:
                            continue
                        keys = measure_config['hbase_keys']
                        columns = measure_config['hbase_columns']
                        temp_table = create_hive_table_from_hbase_table(self.hive, table_name, table_name, keys, columns, self.task_UUID)
                        tables.append(temp_table)
                        #self.context.add_clean_hive_tables(temp_table)
                        tables_energyType.append(energyType)
                        tables_source.append(companyId)
                        self.logger.debug("Created table: {}".format(temp_table))
                    except Exception as e:
                        self.logger.debug("Error creating table: {}".format(e))
            self.logger.debug(len(tables))

            fields = measure_config["hive_fields"]

            location = measure_config['measures'].format(UUID=self.task_UUID)
            #self.context.add_clean_hdfs_file(location)
            input_table = create_hive_module_input_table(self.hive, measure_config['temp_input_table'],
                                                     location, fields, self.task_UUID)

            #add input table to be deleted after execution
            #self.context.add_clean_hive_tables(input_table)
            qbr = RawQueryBuilder(self.hive)
            select = ", ".join([f[0] for f in measure_config["sql_sentence_select"]])
            sentence = """
                INSERT OVERWRITE TABLE {input_table}
                SELECT {select} FROM
                ( """.format(select=select, input_table=input_table)
            letter = ["a{}".format(i) for i in range(len(tables) + 1)]
            text = []
            for index, tab in enumerate(tables):
                var = letter[index]
                energy_type = tables_energyType[index]
                source = tables_source[index]
                select = ", ".join([f[1] for f in measure_config["sql_sentence_select"]]).format(var=var,
                                                                                                 energy_type=energy_type,
                                                                                                 source=source)
                where = measure_config["sql_where_select"].format(var=var, ts_from=ts_from, ts_to=ts_to)
                text.append(""" SELECT {select} FROM {tab} {var}
                                  WHERE
                                      {where}
                                  """.format(var=var, select=select, tab=tab,
                                             where=where))
            sentence += """UNION
                        """.join(text)
            sentence += """) unionResult """

            self.logger.debug(sentence)
            try:
                qbr.execute_query(sentence)
            except:
                continue

        ######################################################################################################################################################################################
        # """ SETUP MAP REDUCE JOB """
        # ######################################################################################################################################################################################
        # output_fields = self.config['output']['fields']
        # clean_tables = []
        # for measure_config in self.config['measures']:
        #     clean_file_name = measure_config['clean_output_file'].format(UUID=self.task_UUID)
        #     self.context.add_clean_hdfs_file(clean_file_name)
        #     clean_table_name = measure_config['clean_output_table']
        #     self.logger.debug('Launching MR job to clean the daily data')
        #     try:
        #         # Launch MapReduce job
        #         self.launcher_hadoop_job(measure_config['type'], measure_config['measures'].format(UUID=self.task_UUID), clean_file_name, result_companyId)
        #     except Exception as e:
        #         raise Exception('MRJob process has failed: {}'.format(e))
        #
        #     clean_table = create_hive_module_input_table(self.hive, clean_table_name,
        #                                                  clean_file_name, output_fields, self.task_UUID)
        #     self.context.add_clean_hive_tables(clean_table)
        #     clean_tables.append([clean_table, measure_config['type']])
        #     self.logger.debug("MRJob finished for {}".format(measure_config['type']))
        #
        # ######################################################################################################################################################################################
        # """ Join the output in a hive table """
        # ######################################################################################################################################################################################
        #
        # output_file_name = self.config['output']['output_file_name']
        # output_hive_name = self.config['output']['output_hive_table']
        # output_hive_table = create_hive_module_input_table(self.hive, output_hive_name,
        #                                                    output_file_name, output_fields)
        # try:
        #     for i in self.hdfs.delete([output_file_name], recurse=True):
        #         try:
        #             i
        #         except:
        #             pass
        # except:
        #     pass
        # select = ", ".join([f[0] for f in self.config['output']["sql_sentence_select"]])
        # sentence = """
        #                 INSERT OVERWRITE TABLE {output_table}
        #                 SELECT {select} FROM
        #                 ( """.format(select=select, output_table=output_hive_table)
        # letter = ["a{}".format(i) for i in range(len(clean_tables) + 1)]
        # text = []
        # for index, tab in enumerate(clean_tables):
        #     var = letter[index]
        #     select = ", ".join([f[1] for f in self.config['output']["sql_sentence_select"]]).format(var=var, data_type=tab[1])
        #     text.append(""" SELECT {select} FROM {tab} {var}
        #                       """.format(var=var, select=select, tab=tab[0]))
        # sentence += """UNION
        #             """.join(text)
        # sentence += """) unionResult """
        #
        # self.logger.debug(sentence)
        # qbr = RawQueryBuilder(self.hive)
        # qbr.execute_query(sentence)
        #
        #
        # self.logger.info('MHbase-HBase ETL clean billing data execution finished...')


if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ETL_clean_daily()
    job.run(commandDictionary)


    """
from module_edinet.edinet_clean_daily_data_etl.task import ETL_clean_daily
from datetime import datetime
params = {
    "result_companyId": "1092915978",
    "ts_to": datetime(2019,7,9)
}
t = ETL_clean_daily()
t.run(params) 
    """