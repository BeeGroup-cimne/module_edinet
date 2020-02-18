from datetime import datetime
import uuid
from os import path
import json
# MongoDB libs
from pymongo import MongoClient
# HDFS libs
from snakebite.client import Client as snakeBiteClient
# HBase libs
import happybase
# HIVE imports
import pyhs2
from hive_functions import delete_hive_table
import logging


class BeeModule2(object):
    @staticmethod
    def date_hook(json_dict):
        for key, value in json_dict.items():
            try:
                json_dict[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
            except:
                pass
        return json_dict

    def __init__(self, module_name, **kwargs):
        self.task_UUID = str(uuid.uuid4()).replace("-","")
        self.module_name = module_name
        self.logger = logging.getLogger('module_name')
        self.logger.addHandler(logging.StreamHandler())
        self.logger.setLevel("DEBUG")
        self.mongo = None
        self.hive = None
        self.hdfs = None
        self.hbase = None
        self.config = self._set_config(module_name, kwargs)
        self.num_reducers = int(self.config["config_execution"]["vcores"] * self.config["config_execution"]["performance"])
        self.report = Report(module=self)
        self.context = Context()



    def _set_config(self, module_name, kwargs):
        self.logger.info('Creating configuration dictionary for module')

        # Create a dict with all config paths to load
        paths_to_join = [path.join(path.dirname(__file__), 'config.json'),
                         path.join(path.dirname(__file__), '{}/config.json'.format(module_name))]

        # Build a result config with all config files latter ones overwrite earlier ones
        config = {}
        for fn in paths_to_join:
            try:
                config_file = open(fn)
            except IOError as e:
                self.logger.debug('No configuration file "{}": {} '.format(fn, e))
                continue
            try:
                # Load json and aggregate to final result
                c = json.load(config_file)
                for k, v in c.items():
                    if k in config:
                        config[k].update(v)
                    else:
                        config[k] = v
            except ValueError as e:
                self.logger.error('Can\'t load configuration file "{}": {} '.format(fn, e))
                raise Exception('Can\'t load configuration file "{}": {} '.format(fn, e))

        # replace config with inline parameters in kwargs
        self._replace_dict(config, UUID=self.task_UUID, **kwargs)
        self.logger.info('Configuration dictionary created successfully for module {} '.format(self.module_name))
        self.logger.debug('Configuration dictionary for module {}: {}'.format(self.module_name, config))
        return config

    def _replace_dict(self,dict_replace, **kwargs):
        for k,v in dict_replace.items():
            try:
                if isinstance(v, dict):
                    self._replace_dict(v, **kwargs)
                elif isinstance(v, list):
                    dict_replace[k] = [x.format(**kwargs) if isinstance(x,str) else x for x in v]
                elif isinstance(v, str) or isinstance(v, unicode):
                    dict_replace[k] = v.format(**kwargs)
            except Exception as e:
                raise Exception(k)

    def _set_mongo(self):
        mongo = MongoClient(self.config['mongodb']['host'],int(self.config['mongodb']['port']))
        mongo[self.config['mongodb']['db']].authenticate(
            self.config['mongodb']['username'],
            self.config['mongodb']['password']
        )

        return mongo ,mongo[self.config['mongodb']['db']]

    def _set_hive(self):
        hive = pyhs2.connect(host=self.config['hive']['host'],
                             port=int(self.config['hive']['port']),
                             authMechanism='PLAIN', user=self.config['hive']['username'], password="",
                             database=self.config['hive']['db'])
        return hive.cursor()

    def _set_hdfs(self):
        hdfs = snakeBiteClient(self.config['hdfs']['host'], int(self.config['hdfs']['port']))
        return hdfs

    def _set_hbase(self):
        hbase = happybase.Connection(self.config['hbase']['host'],
                                     int(self.config['hbase']['port']),
                                     table_prefix=self.config['hbase']['db'],
                                     table_prefix_separator=self.config['hbase']["db_separator"]
        )
        hbase.open()
        return hbase

    def get_connections(self):
        return {
                'hive': self.hive,
                'hdfs': self.hdfs,
                'mongo': self.mongo,
                'hbase': self.hbase
        }

    def _cleanup_temp_data(self, recurse=False):
        # Connect to hdfs with snakebite library
        paths = self.context['clean_hdfs_files']
        try:
            for i in self.hdfs.delete(paths, recurse=recurse):
                try:
                    # Use the cursor just to delete specified path
                    i
                    # logger.debug(i)
                except Exception as e:
                    pass
        except:
            pass
                # raise Exception("Can't delete specified paths '%s': %s " % (paths, e))
        self.logger.debug('Temporary data removed from HDFS')
        for table in self.context['clean_hive_tables']:
            try:
                delete_hive_table(self.hive, table)
                self.logger.debug('Romoved table {}'.format(table))
            except Exception as e:
                self.logger.error("Cant delete table: {}: {}".format(table, e))

    def _start_task(self, params):
        self.logger.info('Setting connections')
        self.mongo_client, self.mongo = self._set_mongo()
        self.logger.info('mongo Ok')

        self.hive = self._set_hive()
        self.logger.info('Hive OK')

        self.hdfs = self._set_hdfs()
        self.logger.info('HDFS OK')

        self.hbase = self._set_hbase()
        self.logger.info('HBASE OK')
        self.logger.info(params)

        self.report.start(params)
        self.logger.info('Starting')


    def _finish_task(self):
        self._cleanup_temp_data(recurse=True)
        self.logger.debug('Closing HIVE, HBASE and MONGO client connections...')
        if self.hbase:
            self.hbase.close()
            self.hbase = None
        if self.hive:
            self.hive.close()
            self.hive = None
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
            self.mongo = None
        self.logger.debug('Connections closed')

    def module_task(self, params):
        raise NotImplementedError("implement in subclass")

    def run(self, params):
        try:
            self._start_task(params)
            self.module_task(params)
            self.report.finish()
        except Exception as e:
            self.logger.debug(e)
            self.report.failed(e)
        self._finish_task()


class Report(dict):
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    FINISHED = "FINISHED"
    def __init__(self, module=None, **kwargs):
        super(Report, self).__init__(kwargs)
        self['task_id'] = module.task_UUID
        self['module_name'] = module.module_name
        self.module = module
        self.mongo_collection = module.config['report']['collection']

    def _save_to_mongo(self):
        db = self.module.mongo[self.mongo_collection]
        if '_id' in self:
            db.update({"_id": self['_id']}, {"$set": self}, upsert=True)
        else:
            self['_id'] = db.insert(self)

    def start(self, params):
        self['started_at'] = datetime.now()
        self['status'] = Report.RUNNING
        self['params'] = params
        self._save_to_mongo()

    def update(self, dict):
        super.update(self, dict)
        # Add of update the datetime of the last update
        self['last_update'] = datetime.now()
        self._save_to_mongo()

    def finish(self):
        # Add or update the datetime when the module is already calculated
        self['finished_at'] = datetime.now()
        self['status'] = Report.FINISHED
        # Pop the last_update variable
        if 'last_update' in self:
            self.pop('last_update')
        self.module.logger.debug('setting finish')
        self._save_to_mongo()

    def failed(self, error):
        self['finished_at'] = datetime.now()
        self['status'] = Report.FAILED
        self['error'] = str(error)
        # Pop the last_update variable
        if 'last_update' in self:
            self.pop('last_update')
        self._save_to_mongo()


class Context(dict):
    """
    class to maintain all status of the module
    """
    def __init__(self, **kwargs):
        super(Context,self).__init__(kwargs)
        self['clean_hdfs_files'] = []
        self['clean_hive_tables'] = []

    def add_clean_hdfs_file(self, file_name):
        self['clean_hdfs_files'].append(file_name)

    def add_clean_hive_tables(self, table_name):
        self['clean_hive_tables'].append(table_name)