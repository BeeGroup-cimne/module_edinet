import cPickle
import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util, BSON
import json
from module_edinet.module_python2 import BeeModule2
import sys

from module_edinet.edinet_mh_hadoop_tertiary_etl.hadoop_job import Hadoop_ETL

class ETL_mh_hadoop_tertiary(BeeModule2):
    """
    ETL to move tertiary consumption from mongo to hadoop
    1. Get the measures from Mongo
    2. Create an HDFS with the measures
    3. Upload the measures to Hbase
    4. Backup the measures uploaded
    5. Delete the uploaded measures from mongo
    """
    def __init__(self):
        super(ETL_mh_hadoop_tertiary, self).__init__("ETL_mh_hadoop_tertiary")
        #delete hdfs directory found in config path on finish

    def create_pickle_file_from_buffer(self, buffer):
        """Creates a temporary CSV file from a list of documents (buffer).
           We need to care with columns order so row_definition must be a list"""
        # Create temporary file on local machine
        file = NamedTemporaryFile(delete=False)
        for doc in buffer:
            file.write(cPickle.dumps(doc).encode('string_escape') + '\r\n')
        file.close()
        return file

    def create_buffer(self, cursor, n):
        """Accumulates cursor documents until we fill the buffer
           or there are no more documents on cursor"""
        buffer = []
        for i in range(n):
            try:
                buffer.append(cursor.next())
            except StopIteration as e:
                break

        return buffer

    def bson_backup(self, collection, query):
        # get the TS before starting the backup so we can format the path where its stored
        dt_to_save = datetime.utcnow()

        # update report with dt_to_save
        filename = '{}[{}].bson'.format(collection, dt_to_save.strftime('%Y-%m-%dT%H-%M-%SZ'))
        folder = '/hdd/backup/mongodb/{}/{}/{}/'.format(collection, str(query['companyId']),
                                                        "%d%02d" % (dt_to_save.year, dt_to_save.month))

        # create folder if not exists
        if not os.path.exists(folder):
            os.makedirs(folder)

        with open('%s%s' % (folder, filename), 'w') as f:

            for doc in self.mongo[collection].find(query):
                # save each doc into file
                # to recover this, we need to use decode_all
                # with open('file') as f:
                #    all_docs = bson.decode_all(f.read())
                f.write(BSON.encode(doc))
        return True

    def hadoop_job(self, input, params):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(self.config))
        f.close()
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))

        # create hadoop job instance adding file location to be uploaded
        dtnow = datetime.now()
        str_dtnow = str(dtnow.year) + str("%02i" % dtnow.month) + str("%02i" % dtnow.day) + str(
            "%02i" % dtnow.hour) + str("%02i" % dtnow.minute)
        mr_job = Hadoop_ETL(args=['-r', 'hadoop', input, '--file', f.name, '--output-dir',
                                  '/tmp/ETL_measures_not_considered/{}/{}'.format(str(params['companyId']), str_dtnow),
                                  '-c', 'module_edinet/edinet_mh_hadoop_tertiary_etl/mrjob.conf'])
        # mr_job = Hadoop_ETL(args=['-r', 'hadoop', input, '--file', f.name, '--python-archive', path.dirname(lib.__file__)])
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                f.unlink(f.name)
                raise Exception('Error running MRJob ETL process using hadoop: {}'.format(e))

        f.unlink(f.name)
        self.logger.debug('Temporary config file uploaded has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report

    def module_task(self, params):

        self.logger.info('Starting MongoDB-HBase ETL using Hadoop ...')

        """CHECK INCONSISTENCIES IN params"""
        try:
            ts_to = params['ts_to'] if 'ts_to' in params else None
            ts_from = params['ts_from'] if 'ts_from' in params else None
            companyId = params['companyId'] if 'companyId' in params else None
            buffer_size = params['buffer_size'] if 'buffer_size' in params else 1000000
            delete_process = params['delete_measures_in_mongo'] if 'delete_measures_in_mongo' in params else True
            timezone = params['timezone'] if 'timezone' in params else 'Europe/Madrid'

        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        """
            QUERY THE MEASURES FROM REST"""

        # set query dictionary
        query = {}
        if companyId is not None:
            query.update({'companyId': companyId})

        if ts_from is not None and ts_to is not None:
            query.update({
                'timestamp': {
                    '$gte': ts_from,
                    '$lt': ts_to
                }
            })

        # set projection dictionary (1 means field returned, 0 field wont be returned)
        projection = {
            'reading': 1,
            'deviceId': 1,
            'timestamp': 1,
            'companyId': 1,
            'values': 1,
            '_id': 0
        }

        # setting variables for readability
        measures_collection = self.config['mongodb']['collection']
        self.logger.debug('Querying for measures in MongoDB: {}'.format(query))
        cursor = self.mongo[measures_collection].find(query, projection)

        """
           LOAD THE SELECTED MEASURES TO HBASE"""

        self.logger.debug('Creating buffer to execute hadoop job. Buffer size: {}'.format(buffer_size))

        buffer = self.create_buffer(cursor, buffer_size)
        while buffer:
            # Copy buffer to hdfs
            file = self.create_pickle_file_from_buffer(buffer)
            try:
                # Launch MapReduce job
                ## Buffered measures to HBase
                self.logger.debug('Loading the buffer to Hbase')
                job_report = self.hadoop_job(file.name, params)
            except Exception as e:
                raise Exception('MRJob process on MongoDB-HBase ETL job has failed: {}'.format(e))

            self.logger.info('A Hadoop job performing ETL process has finished. Loaded {} measures'.format(len(buffer)))

            # remove temporary file
            self.logger.debug('Removing temporary local CSV file: {}'.format(file.name))
            file.unlink(file.name)

            # create new buffer for next loop
            del buffer
            self.logger.debug('Creating buffer to execute hadoop job. Buffer size: {}'.format(buffer_size))
            buffer = self.create_buffer(cursor, buffer_size)

        """
         DELETE THE REST MEASURES THAT HAS BEEN UPLOADED TO HBASE"""

        if delete_process:
            try:
                self.logger.info('Making the backup of the REST measures that has been loaded to Hbase')
                self.bson_backup(self.mongo, measures_collection, query)
                self.logger.info('The backup finished successfully')
            except Exception as e:
                raise Exception('There was an error with the backup process of the REST measures: {}'.format(e))
            try:
                self.logger.info('Deleting the REST measures that has been loaded to Hbase')
                self.mongo[measures_collection].remove(query)
                self.logger.info('The measures from REST were deleted')
            except Exception as e:
                raise Exception('There was an error with the deleting process of the REST measures: {}'.format(e))


if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ETL_mh_hadoop_tertiary()
    job.run(commandDictionary)


    """
from module_edinet.edinet_mh_hadoop_tertiary_etl.task import ETL_mh_hadoop_tertiary
from datetime import datetime
params = {
    'companyId': 3363400001,
    'buffer': 1000000,
    'delete_in_mongo': False
}
t = ETL_mh_hadoop_tertiary()
t.run(params) 
    """