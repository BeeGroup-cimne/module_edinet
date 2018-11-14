import itertools
import os
import subprocess
from datetime import datetime
from tempfile import NamedTemporaryFile
from bson import json_util
import types
import json
from list_functions import unique
from hive_functions.query_builder import QueryBuilder
from hive_functions import create_hive_module_input_table, create_hive_table_from_hbase_table
from module_edinet.module_python2 import BeeModule2
from module_edinet.edinet_delete_measures.hadoop_job import MRJob_delete_measures
import sys


class DeleteMeasuresModule(BeeModule2):
    """
    DELETE_MEASURES_EDINET MODULE
    1. READ THE DOCUMENTS IN delete_measures COLLECTION THAT HAVE status:false
    2. ORDER EVERY COMBINATION OF type_company AND ts_from-ts_to
    3. SELECT THE DATA TO BE DELETED. (BUFFER ALL THE KEYS THAT HAVE TO BE DELETED FROM HBASE)
    4. JOINING OF THE HBASE KEYS TO DELETE
    5. MR PROCESS TO DELETE THE KEYS FROM HBASE
    6. DELETE HIVE TEMPORARY TABLES
    7. DELETE THE TEMPORARY HDFS DIRECTORY
    8. UPDATE THE REPORT ON THE delete_measures COLLECTION
    """
    def __init__(self):
        super(DeleteMeasuresModule, self).__init__("edinet_delete_measures")
        self.context.add_clean_hdfs_file(self.config['path']['result'])

    def hadoop_job(self, context, input):
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

        # create hadoop job instance adding file location to be uploaded
        mr_job = MRJob_delete_measures(
            args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '-c', 'module_edinet/edinet_delete_measures/mrjob.conf'])
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
        self.logger.info('Starting Module for edinet delete measures...')
        starting_at = datetime.now()
        """CHECK INCONSISTENCIES IN params"""
        try:
            company = params['companyId'] if 'companyId' in params else None
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        #####################################################################################################################################################################################
        """READ THE DELETE ORDERS"""
        ######################################################################################################################################################################################
        self.logger.info('Reading the delete orders...')

        collection = self.config['mongodb']['collection']
        cursor = self.mongo[collection].find({'report.status': False, 'companyId': company})
        buffer_docs = []
        possible_types = self.config['types']

        # Look for the available tables
        tables_all = self.hbase.tables()

        for docs in cursor:
            if 'type' not in docs or docs['type'] is None:
                docs['type'] = [item for item in possible_types if item + '_' + str(company) in tables_all]
            elif isinstance(docs['type'], types.StringType):
                docs['type'] = [docs['type']]
            buffer_docs.append(docs)

        ######################################################################################################################################################################################
        """ORDER EVERY COMBINATION"""
        ######################################################################################################################################################################################

        if len(buffer_docs) == 0:
            self.logger.info("There are not any delete orders")
            return True

        self.logger.info('There are delete orders to do')
        self.logger.info('Building the general condition to select the results to delete')
        possible_typcom = {}
        for item in buffer_docs:
            for type_item in item['type']:
                possible_typcom[str(type_item + '_' + str(item['companyId']))] = 1
        possible_typcom = list(possible_typcom.keys())

        # Split by type_company
        typcom = {}
        for type_company in possible_typcom:
            values = []
            for item in buffer_docs:
                for type_item in item['type']:
                    if type_company == type_item + '_' + str(item['companyId']):
                        # Assign deviceId as a list of devices or a None value
                        value_deviceId = [str(item['deviceId'])]
                        if isinstance(item['deviceId'], types.ListType):
                            value_deviceId = [str(item_device) for item_device in item['deviceId']]
                        # logger.info('VALUESdeviceid : %s' %(value_deviceId) )
                        # logger.info('device Id : %s' %(item['deviceId']) )
                        value = {
                            'deviceId': value_deviceId,
                            'ts_to': item['ts_to'] if 'ts_to' in item else None,
                            'ts_from': item['ts_from'] if 'ts_from' in item else None
                        }
                        values.append(value)
                    self.logger.info('VALUES : %s, TYPE_COMPANY: %s'.format(values, type_company))
                    if values != []:
                        typcom[type_company] = values

        typcomper = {}
        # Split by type_company and period combination
        for type_company, list_per_typcom in typcom.iteritems():
            typcomper[type_company] = {}
            # Combination of ts_from and ts_to for this type_company
            period_comb = []
            for item in list_per_typcom:
                if item['ts_from'] is not None and item['ts_to'] is not None:
                    period_comb.append(item['ts_from'].strftime("%s") + '-' + item['ts_to'].strftime("%s"))
                elif item['ts_from'] is None and item['ts_to'] is not None:
                    period_comb.append('None-' + item['ts_to'].strftime("%s"))
                elif item['ts_from'] is not None and item['ts_to'] is None:
                    period_comb.append(item['ts_from'].strftime("%s") + '-None')
                else:
                    period_comb.append('None-None')
            for period in period_comb:
                values = []
                for item in list_per_typcom:
                    if item['ts_from'] is not None and item['ts_to'] is not None:
                        period_item = item['ts_from'].strftime("%s") + '-' + item['ts_to'].strftime("%s")
                    elif item['ts_from'] is None and item['ts_to'] is not None:
                        period_item = 'None-' + item['ts_to'].strftime("%s")
                    elif item['ts_from'] is not None and item['ts_to'] is None:
                        period_item = item['ts_from'].strftime("%s") + '-None'
                    else:
                        period_item = 'None-None'
                    if period_item == period:
                        values.append(item['deviceId'])
                if None in values:
                    values = None
                else:
                    values = unique(list(itertools.chain.from_iterable(values)))
                typcomper[type_company][period] = values

        ######################################################################################################################################################################################
        """SELECT THE DATA TO BE DELETED. (BUFFER ALL THE KEYS THAT HAVE TO BE DELETED FROM HBASE)"""
        ######################################################################################################################################################################################

        """
        Example of the document that we will iterate, generating the key - table output needed to delete the results in Hbase.
        typcomper = {
                        'electricityConsumption_1234509876': {
                            '1422745200-1424991600': [None]
                        }
                    }
        """
        elements_deleted = {}
        multiple_table_from = []
        for type_company, dict_to_delete in typcomper.iteritems():
            # Initialize the elements deleted dictionary for this type_company
            elements_deleted[type_company] = {}

            ###########################################################################################
            # Definition of the condition for the HIVE query
            ###########################################################################################

            condition_hive = []

            # Period definition
            for period, devices_to_delete in dict_to_delete.iteritems():

                condition_period = []  ####INITIALIZE

                time_from = period.split("-")[0] if period.split("-")[0] != 'None' else None
                time_to = period.split("-")[1] if period.split("-")[1] != 'None' else None
                if time_from is not None:
                    condition_period.append('r.key.ts>=' + time_from)
                if time_to is not None:
                    condition_period.append('r.key.ts<=' + time_to)

                if devices_to_delete:
                    condition_period.append('r.key.deviceId in ("%s")' % ('","'.join(devices_to_delete)))

                if condition_period != []:
                    condition_hive.append('(%s)' % " and ".join(condition_period))

            # Final HIVE condition
            if condition_hive != []:
                condition = "(%s)" % (" or ".join(condition_hive))
            else:
                condition = None

            ###########################################################################################
            # Query to found the keys that we have to delete
            ###########################################################################################

            # Test if there is an existing Hbase table for this type_company
            exist_hbase_table = True if type_company in context['clients']['hbase'].tables() else False

            if exist_hbase_table:

                self.logger.info(
                    "Running the HIVE query to detect the keys to delete in the table {}. The condition is the following: --{}--".format(
                    type_company, str(condition)))

                # Create and run the Hive query to Hbase
                qb = QueryBuilder(self.hive)

                hive_keys = {"b": "tinyint", "ts": "bigint", "deviceId": "string"}
                columns = [("value", "float", "m:v"), ("accumulated", "float", "m:va")]
                table_from = create_hive_table_from_hbase_table(self.hive, type_company, type_company,
                                                                hive_keys, columns, self.task_UUID)
                self.context.add_clean_hive_tables(table_from)
                location = self.config['paths']['result'] + '/' + type_company
                fields = [('key', 'string'), ('hbase_table', 'string'), ('value', 'string'), ('accumulated', 'string')]
                table_input = create_hive_module_input_table(self.hive, '{}_keys_to_delete'.format(type_company),
                                                             location, fields, )
                self.context.add_clean_hive_tables(table_input)

                qb = qb.add_from(table_from, 'r').add_insert(table=table_input)
                qb = qb.add_select(
                    'concat_ws("~",cast(r.key.b as string),cast(r.key.ts as string),r.key.deviceId),"{}", r.value, r.accumulated '.format(type_company))
                if condition is not None:
                    qb = qb.add_where('{}'.format(condition))
                # qb = qb.add_groups(['r.value'])
                qb.execute_query()
                multiple_table_from.append(table_input)

                # Count the keys to delete
                location_count = self.config['paths']['count']
                table_input_count = create_hive_module_input_table(self.hive, '{}_keys_to_delete_counter'.format(type_company),
                                                                   location_count,[('count', 'int')], self.task_UUID)

                self.context.add_clean_hive_tables(table_input_count)
                qb = QueryBuilder(self.hive)
                qb = qb.add_from(table_input, 'r').add_insert(table=table_input_count)
                qb = qb.add_select('COUNT(r.key)')
                qb = qb.add_groups(['r.hbase_table'])
                qb.execute_query()

                aux = subprocess.Popen(
                    ["hadoop", "fs", "-cat", context['config']['module']['paths']['count'] + '/000000_0'],
                    stdout=subprocess.PIPE).stdout.readlines()

                if len(aux) > 0:
                    elements_deleted[type_company]['hbase'] = int(aux[0].replace("\n", ""))
                else:
                    elements_deleted[type_company]['hbase'] = 0

        ######################################################################################################################################################################################
        """QUERY TO JOIN ALL THE RESULTS"""
        ######################################################################################################################################################################################

        self.logger.info("Running the HIVE query to join all the keys to delete in a single file")
        self.logger.info("{}".format(typcomper))
        # Test if there are results to delete
        keys_selected = 0
        for type_company in typcomper.iterkeys():
            keys_selected += elements_deleted[type_company]['hbase']

        if keys_selected > 0:
            qb = QueryBuilder(self.hive)
            fields = [('key', 'string'), ('hbase_table', 'string')]
            union_all_query = '(' + ' UNION ALL '.join(
                [str('SELECT * FROM {}'.format(table_from)) for table_from in multiple_table_from]) + ')'
            table_input_final = create_hive_module_input_table(self.hive, '{}_keys_to_delete_ALL'.format(type_company),
                                                               self.config['paths']['result_final'],
                                                               fields, self.task_UUID)
            self.context.add_clean_hive_tables(table_input_final)
            qb = qb.add_from(union_all_query, 'r').add_insert(table=table_input_final)
            qb = qb.add_select('r.key,r.hbase_table')
            qb.execute_query()

        ######################################################################################################################################################################################
        """MR PROCESS TO DELETE THE KEYS FROM HBASE"""
        ######################################################################################################################################################################################

        self.logger.info("Processing a MRjob that deletes all the selected keys")

        if keys_selected > 0:
            try:
                self.hadoop_job(
                    context, context['config']['module']['paths']['result_final'])
            except Exception as e:
                raise Exception('MRJob process on delete_measures_edinet has failed: {}'.format(e))


        ######################################################################################################################################################################################
        """UPDATE THE REPORT OF THE ORDERS IN THE delete_measures COLLECTION"""
        ######################################################################################################################################################################################

        self.logger.info("Delete the HDFS temporary files")

        for doc in buffer_docs:
            mongo_status = doc['report']['deleted_old_data_from_mongo'] if 'deleted_old_data_from_mongo' in doc[
                'report'] else False
            self.mongo[collection].update({'_id': doc['_id']},
                                          {
                                              '$set': {
                                                  'report': {
                                                      'status': True,
                                                      'deleted_old_data_from_mongo': mongo_status,
                                                      'started_at': starting_at,
                                                      'finished_at': datetime.now()
                                                  }
                                              }
                                          }
                                          )
        return True

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = DeleteMeasuresModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_delete_measures.task import DeleteMeasuresModule
from datetime import datetime
params = {
   'companyId': 1092915978,
}
t = DeleteMeasuresModule()
t.run(params) 
    """