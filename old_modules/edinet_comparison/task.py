import itertools
import json
import sys
from subprocess import call
from tempfile import NamedTemporaryFile

from bson import json_util
from dateutil.relativedelta import relativedelta

from module_edinet.edinet_comparison.align_job import MRJob_align
from module_edinet.edinet_comparison.py.load_to_dbs import LoadResultsToRESTandHBase
from module_edinet.edinet_comparison.py.similars_best_criteria import SimilarsBestCriteria
from module_edinet.edinet_comparison.py.similars_distribution import SimilarsDistribution
from module_edinet.module_python2 import BeeModule2
from hive_functions import create_hive_module_input_table, create_hive_table_from_hbase_table
from hive_functions.query_builder import RawQueryBuilder, QueryBuilder
from datetime_functions import date_n_month, last_day_n_month
from datetime import datetime

class ComparisonModule(BeeModule2):
    """
    Calculates the comparison modules with the buildings of edinet
    1. Get the modelling_units documents of mongo by type to compare.
    2. Calculate the comparisons for each group
    3. Select the best group criteria fore each building
    """
    def __init__(self):
        super(ComparisonModule, self).__init__("edinet_comparison")
        #delete hdfs directory found in config path on finish
        self.context.add_clean_hdfs_file(self.config['paths']['all'])

    def launcher_hadoop_job(self, type, input, output=None, company=None, devices=None, fields=None):
        """Runs the Hadoop job uploading task configuration"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({'devices': devices, 'company': company, 'fields': fields})
        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()
        report['config_temp_file'] = f.name
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))

        # create hadoop job instance adding file location to be uploaded
        mr_job = MRJob_align(
            args=['-r', 'hadoop', 'hdfs://' + input, '--file', f.name, '--output-dir', output,
                  '-c', 'module_edinet/edinet_comparison/mrjob.conf'])
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

    def similars_distribution(self, input_path, output_path, columns, variables, criteria, min_consumption_percentile,
                              max_consumption_percentile, group_volume_limits, group_volume_penalties,
                              keys_for_filtering_and_best_criteria,
                              key_yearmonths='m', sep='\t'):

        """Consider the timeSlotName for each hourly measure"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input_path,
            'output': output_path
        }
        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({
            'columns': columns,
            'variables': variables,
            'criteria': criteria,
            'key_yearmonths': key_yearmonths,
            'sep': sep,
            'min_consumption_percentile': min_consumption_percentile,
            'max_consumption_percentile': max_consumption_percentile,
            'group_volume_limits': group_volume_limits,
            'group_volume_penalties': group_volume_penalties,
            'keys_for_filtering_and_best_criteria': keys_for_filtering_and_best_criteria
        })

        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()

        report['config_temp_file'] = [f.name]
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))

        # create hadoop job instance adding file location to be uploaded
        mr_job = SimilarsDistribution(args=['-r', 'hadoop', 'hdfs://' + input_path,
                                            '--output-dir', 'hdfs:///' + output_path,
                                            '--file', f.name,
                                            '-c', 'module_edinet/edinet_comparison/mrjob.conf',
                                            '--jobconf', 'mapred.reduce.tasks=8',
                                            '--jobconf', 'mapred.task.timeout=3600000'])
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                f.unlink(f.name)
                raise Exception('Error running SimilarsDistribution process using hadoop: {}'.format(e))

        f.unlink(f.name)
        self.logger.debug('Temporary config files has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report


    def similars_best_criteria(self, input_path, input_similars_path, output_path, columns, columns_similars,
                               criteria, min_yearmonths, n_months, keys_for_filtering_and_best_criteria,
                               fixed_part, key_yearmonths='m', sep='\t', sep_similars='\t'):

        """Consider the timeSlotName for each hourly measure"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input_path,
            'output': output_path
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({
            'columns': columns,
            'columns_similars': columns_similars,
            'criteria': criteria,
            'key_yearmonths': key_yearmonths,
            'min_yearmonths': min_yearmonths,
            'fixed_part': fixed_part,
            'n_months': n_months,
            'sep': sep,
            'sep_similars': sep_similars,
            'keys_for_filtering_and_best_criteria': keys_for_filtering_and_best_criteria
        })

        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()

        # Join the similar groups distribution file
        s = NamedTemporaryFile(delete=False, suffix='.similars')
        call(['hadoop', 'fs', '-getmerge', 'hdfs://{}'.format(input_similars_path), s.name])
        s.close()

        report['config_temp_file'] = [f.name]
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))

        # create hadoop job instance adding file location to be uploaded
        mr_job = SimilarsBestCriteria(args=['-r', 'hadoop', 'hdfs://' + input_path,
                                            '--output-dir', 'hdfs:///' + output_path,
                                            '--file', f.name,
                                            '--file', s.name,
                                            '-c', 'module_edinet/edinet_comparison/mrjob.conf',
                                            '--jobconf', 'mapred.task.timeout=3600000'])
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                f.unlink(f.name)
                s.unlink(s.name)
                raise Exception('Error running SimilarsBestCriteria process using hadoop: {}'.format(e))

        f.unlink(f.name)
        s.unlink(s.name)
        self.logger.debug('Temporary config files has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report

    def load_to_dbs(self, input_path, type, companyId, input_fields, output_hbase_key, output_hbase_fields,
                    output_mongo_key,
                    output_mongo_fields, output_hbase_table, output_mongo_collection,
                    special_functions_for_input_fields=None,
                    create_hbase_column_family_if_not_exists=None, case=None, operation=None, mongo_operation="update",
                    delete_old_mongo_items=None, sep="\t", n_reducers=4):

        """Load the results to the db"""
        # create report to save on completion or error
        report = {
            'started_at': datetime.now(),
            'state': 'launched',
            'input': input_path
        }

        # Create temporary file to upload with json extension to identify it in HDFS
        job_extra_config = self.config.copy()
        job_extra_config.update({
            'sep': sep,
            'n_reducers': n_reducers,
            'type': type,
            'companyId': companyId,
            'case': case,
            'operation': operation,
            'input_fields': input_fields,
            'output_hbase_key': output_hbase_key,
            'output_hbase_fields': output_hbase_fields,
            'output_mongo_key': output_mongo_key,
            'output_mongo_fields': output_mongo_fields,
            'output_hbase_table': output_hbase_table,
            'output_mongo_collection': output_mongo_collection,
            'create_hbase_column_family_if_not_exists': create_hbase_column_family_if_not_exists,
            'mongo_operation': mongo_operation,
            'delete_old_mongo_items': delete_old_mongo_items,
            'special_functions_for_input_fields': special_functions_for_input_fields})

        f = NamedTemporaryFile(delete=False, suffix='.json')
        f.write(json.dumps(job_extra_config))
        f.close()

        report['config_temp_file'] = [f.name]
        self.logger.debug('Created temporary config file to upload into hadoop and read from job: {}'.format(f.name))

        # create hadoop job instance adding file location to be uploaded
        mr_job = LoadResultsToRESTandHBase(args=['-r', 'hadoop', 'hdfs://' + input_path,
                                                 '--file', f.name,
                                                 '-c', 'module_edinet/edinet_comparison/mrjob.conf',
                                                 '--jobconf', 'mapred.reduce.tasks={}'.format(str(n_reducers)),
                                                 '--jobconf', 'mapred.task.timeout=3600000'])
        # mr_job = LoadResultsToRESTandHBase(args=['-r', 'hadoop', 'hdfs://'+input_path, '--file', f.name, '--python-archive', path.dirname(lib.__file__), '--output-dir', '/tmp/prova_contracts_plus'])
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            except Exception as e:
                f.unlink(f.name)
                raise Exception('Error running LoadResultsToRESTandHBase process using hadoop: {}'.format(e))

        f.unlink(f.name)
        self.logger.debug('Temporary config files has been deleted from FileSystem')

        report['finished_at'] = datetime.now()
        report['state'] = 'finished'

        return report


    def module_task(self, params):
        self.logger.info('Starting Module for edinet comparisons ...')
        """CHECK INCONSISTENCIES IN params"""
        try:
            companyId = params['companyId'] if 'companyId' in params else None
            companyId_toJoin = params['companyId_toJoin'] if 'companyId_toJoin' in params else []
            companyId_toJoin = companyId_toJoin if isinstance(companyId_toJoin, list) else [companyId_toJoin]
            timezone = params['timezone'] if 'timezone' in params else 'Europe/Madrid'
            ts_to = params['ts_to']
            energyType = params['type']
            # I need just one energy type, so the first should be the main energy type
            mainEnergyType = energyType[0] if isinstance(energyType, list) else energyType
            ts_from = params['ts_from'] if 'ts_from' in params else date_n_month(ts_to, -42)
            eff_users = float(params['eff_users']) / 100 if params['eff_users'] >= 1 else params['eff_users']
            criteria = sorted([sorted(criteria_set.split(" + ")) for criteria_set in params['criteria']])
            triggerValue = params['triggerValue'] if 'triggerValue' in params else 25
            # quantile_min = params['quantile_min'] if 'quantile_min' in params else 0.05
            # quantile_max = params['quantile_max'] if 'quantile_max' in params else 0.95
            dailyResampling_company = True
            similar_users_group_calculation = params[
                'similar_users_group_calculation'] if 'similar_users_group_calculation' in params else True
            min_consumption_percentile = params[
                'min_consumption_percentile'] if 'min_consumption_percentile' in params else 5
            max_consumption_percentile = params[
                'max_consumption_percentile'] if 'max_consumption_percentile' in params else 95
            n_months_for_best_criteria_calc = self.config['settings']['similar_users_groups'][
                'n_months_for_best_criteria']
            fixed_percentage_for_cost = self.config['settings']['similar_users_groups'][
                'fixed_percentage_for_cost']
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        #####################################################################################################################################################################################
        """  LOAD from MONGO to HBASE  """
        ######################################################################################################################################################################################

        self.logger.info('Extracting data from mongodb')

        # set query dictionary
        if params['companyId']:
            query = {'companyId': params['companyId']}
        else:
            query = {}

        # set projection dictionary (1 means field returned, 0 field wont be returned)
        projection = {
            '_id': 0,
            '_updated': 0,
            '_created': 0
        }

        # setting variables for readability
        collection = self.config['mongodb']['modelling_units_collection']

        self.logger.debug('Querying for modelling units in MongoDB: {}'.format(query))
        cursor = self.mongo[collection].find(query, projection)

        def type_in_energyType(type_modelling_unit, type_parameters):
            type_to_compare = {
                'gasConsumption': ['heatConsumption', 'gasConsumption'],
                'electricityConsumption': ['electricityConsumption', 'tertiaryElectricityConsumption']
            }
            for e_type in type_parameters:
                if e_type in type_to_compare.keys():
                    if type_modelling_unit in type_to_compare[e_type]:
                        return True
                    else:
                        return False
            return False

        if not isinstance(energyType, list):
            energyType = [energyType]
        device_key = {}
        for item in cursor:
            if len(item['devices']) > 0:  # to avoid empty list of devices
                model = str(item['baseline']['model']) if 'baseline' in item and 'model' in item[
                    'baseline'] else 'Weekly30Min'
                # search for reporting_collections that contain energyType information "GREAME"
                reporting_collection = self.config['mongodb']['reporting_collection']
                reporting_unit = self.mongo[reporting_collection].find_one(
                    {"modelling_Units": item["modellingUnitId"], 'companyId': params['companyId']}, projection)
                type_energy = None
                if reporting_unit:
                    try:
                        modeling_unit_type = {k: v for k, v in
                                              zip(reporting_unit["modelling_Units"], reporting_unit["typeList"])}
                        type_energy = modeling_unit_type[item["modellingUnitId"]]
                    except:
                        type_energy = "Discard Energy"

                # Nomes poso els devices de edinet que tenint type coincideix amb el que estem calculant
                add_devices = False
                if type_energy:
                    if type_in_energyType(type_energy, energyType):
                        add_devices = True
                else:
                    add_devices = True
                if add_devices:
                    for dev in item['devices']:
                        if str(dev['deviceId'].encode('utf-8')) in device_key.keys():
                            device_key[str(dev['deviceId'].encode('utf-8'))].append(
                                str(item['modellingUnitId']) + '~' + str(item['devices']) + '~' + model)
                        else:
                            device_key[str(dev['deviceId'].encode('utf-8'))] = [
                                str(item['modellingUnitId']) + '~' + str(item['devices']) + '~' + model]

        self.logger.info('A mongo query process has loaded {} devices'.format(len(device_key.keys())))

        ######################################################################################################################################################################################
        """ HIVE QUERY TO PREPARE THE DATA """
        ######################################################################################################################################################################################
        """ CRITERIAS """
        # Joining the different list of criterias to eliminate repeated ones
        merged = list(itertools.chain.from_iterable(criteria))
        all_criteria_fields = list(set(merged))

        """ VALUES """
        companyId_toJoin.append(companyId)
        tables = []
        energyTypeList = []
        for i in range(len(energyType)):
            for j in range(len(companyId_toJoin)):
                try:
                    table_name = "{}_{}".format(energyType[i], companyId_toJoin[j])
                    hive_keys = {"b": "tinyint", "ts": "bigint", "deviceId": "string"}
                    columns = [("value", "float", "m:v"), ("accumulated", "float", "m:va")]
                    temp_table = create_hive_table_from_hbase_table(self.hive, table_name, table_name, hive_keys, columns, self.task_UUID)
                    tables.append(temp_table)
                    self.context.add_clean_hive_tables(temp_table)
                    energyTypeList.append(energyType[i])
                except:
                    pass
        self.logger.debug(len(tables))

        fields = [('deviceId', 'string'), ('ts', 'int'), ('value', 'float'), ('accumulated', 'float'),
                  ('energyType', 'string')]
        location = self.config['paths']['measures']
        input_table = create_hive_module_input_table(self.hive, 'edinet_comparison_input', location,
                                                     fields, self.task_UUID)
        # add input table to be deleted after execution
        self.context.add_clean_hive_tables(input_table)
        qbr = RawQueryBuilder(self.hive, self.logger)

        sentence = """
        INSERT OVERWRITE TABLE {input_table}
        SELECT deviceId, ts, value, accumulated, energyType FROM
        ( """

        letter = ''.join(chr(ord('a') + i) for i in range(len(tables) + 1))
        text = []
        for index, tab in enumerate(tables):
            var = letter[index]
            energy_type = energyTypeList[index]
            text.append(""" SELECT {var}.key.deviceId, {var}.key.ts, {var}.value, {var}.accumulated, '{energy_type}' as energyType FROM {tab} {var}
                                          WHERE
                                              {var}.key.ts >= UNIX_TIMESTAMP("{ts_from}","yyyy-MM-dd HH:mm:ss") AND
                                              {var}.key.ts <= UNIX_TIMESTAMP("{ts_to}","yyyy-MM-dd HH:mm:ss") AND
                                              {var}.key.deviceId IN {devices}
                                          """.format(var=var, energy_type=energy_type, tab=tab,
                                                     ts_from="{ts_from}", ts_to="{ts_to}", devices="{devices}"))
        sentence += """UNION ALL
                            """.join(text)
        sentence += """) unionResult """
        vars = {
            'input_table': input_table,
            'ts_to': ts_to,
            'ts_from': ts_from,
            'devices': tuple(device_key.keys()) if len(device_key.keys()) > 1 else "('" + ",".join(
                device_key.keys()) + "')"
        }

        self.logger.debug(sentence.format(**vars))
        qbr.execute_query(sentence.format(**vars))

        """
        SETUP MAP REDUCE JOB
        """

        self.logger.info('Getting')
        location_joined = self.config['paths']['joined']
        try:
            # Launch MapReduce job
            self.logger.debug('MRJob Align')
            self.launcher_hadoop_job('align', location, location_joined, companyId, device_key, all_criteria_fields)

        except Exception as e:
            raise Exception('MRJob ALIGN process job has failed: {}'.format(e))

        self.logger.info('Align mrjob execution finished...')

        # list of fields needed into the next calculations
        fields = ['modellingUnitId', 'rawMonths']
        fields.extend(item for item in all_criteria_fields)

        # SIMILAR USERS GROUPS
        # [Python Map Reduce] Calculate the summary statistics for each possible similar users criteria group
        location_similar_results = self.config['paths']['results_similarUsers']
        try:
            if similar_users_group_calculation:
                call(["hadoop", "fs", "-rm", "-r", location_similar_results])
                self.similars_distribution(
                    input_path=location_joined,
                    output_path=location_similar_results,
                    columns=fields,
                    criteria=criteria,
                    variables=['rawMonths'],
                    min_consumption_percentile=min_consumption_percentile,
                    max_consumption_percentile=max_consumption_percentile,
                    group_volume_limits=[60, 2000, 8000],
                    group_volume_penalties=[50, 3],
                    keys_for_filtering_and_best_criteria=['rawMonths', 'v']
                )
        except Exception as e:
            raise Exception('MRJob errors: {}'.format(e))

        self.logger.debug("similar_distribution mrjob execution finished...")

        # SIMILAR USERS GROUPS
        # 13.4. [Python Map Reduce] Load the similar users distribution results to REST and HBase databases
        try:
            if similar_users_group_calculation:
                self.load_to_dbs(
                    input_path=location_similar_results,
                    type=mainEnergyType,
                    companyId=companyId,
                    input_fields=['criteria', 'groupCriteria', 'month', 'pCust', 'nCust', 'cDisp', 'cVar', 'avg',
                                  'results'],
                    output_hbase_key=['month',
                                      'companyId',
                                      'type',
                                      'criteria',
                                      'groupCriteria'],
                    output_hbase_fields=[('r:average', 'avg', 'string'),
                                         ('r:results', 'results', 'string'),
                                         ('r:penalty', 'pCust', 'float'),
                                         ('r:numberCustomers', 'nCust', 'int'),
                                         ('r:coefficientVariation', 'cVar', 'float'),
                                         ('r:coefficientDispersion', 'cDisp', 'float')],
                    output_mongo_key=[('month', 'month', 'int'),
                                      ('companyId', 'companyId', 'int'),
                                      ('type', 'type', 'string'),
                                      ('criteria', 'criteria', 'string'),
                                      ('groupCriteria', 'groupCriteria', 'string')],
                    output_mongo_fields=[('average', 'avg', 'json'),
                                         ('results', 'results', 'json'),
                                         ('penalty', 'pCust', 'float'),
                                         ('numberCustomers', 'nCust', 'int'),
                                         ('coefficientVariation', 'cVar', 'float'),
                                         ('coefficientDispersion', 'cDisp', 'float')],
                    output_hbase_table=self.config['settings']['similar_users_groups'][
                        'hbase_table_dist'],
                    output_mongo_collection=self.config['settings']['similar_users_groups'][
                        'mongo_collection_dist'],
                    create_hbase_column_family_if_not_exists={'r': dict()},
                    mongo_operation="update"
                )

        except Exception as e:
            raise Exception('Load to DB errors: {}'.format(e))

        # Create the static HIVE table to become available all the summary results for each possible similar users criteria
        if similar_users_group_calculation:
            create_hive_table_from_hbase_table(
                self.hive,
                table_hive=self.config['settings']['similar_users_groups']['hbase_table_dist'],
                table_hbase=self.config['settings']['similar_users_groups']['hbase_table_dist'],
                hive_key={'month':'int', 'companyId':'bigint', 'type':'string', 'criteria':'string', 'groupCriteria':'string'},
                columns=[('average', 'string', 'r:average'),
                         ('results', 'string', 'r:results'),
                         ('penalty', 'float', 'r:penalty'),
                         ('numberCustomers', 'int', 'r:numberCustomers'),
                         ('coefficientVariation', 'float', 'r:coefficientVariation'),
                         ('coefficientDispersion', 'float', 'r:coefficientDispersion')
                        ]
            )
        self.context.add_clean_hive_tables(self.config['settings']['similar_users_groups']['hbase_table_dist'])

        self.logger.info("created hive_table")
        # SIMILAR USERS GROUPS
        # 13.5. [Hive] Query the variables needed for the best criteria consideration from the similar distribution table.

        qb = QueryBuilder(self.hive, self.logger)
        fields_similars_bc = [('month', 'int'), ('criteria', 'string'), ('groupCriteria', 'string'),
                              ('average', 'string'),
                              ('penalty', 'float'), ('numberCustomers', 'int'), ('coefficientVariation', 'float'),
                              ('coefficientDispersion', 'float')]
        table_similars_for_best_criteria = create_hive_module_input_table(
            self.hive,
            'EDINET_SimilarUsersGroups_DistributionForBestCriteria',
            location_similar_results + '_bc',
            fields_similars_bc,
            self.task_UUID)
        self.context.add_clean_hive_tables(table_similars_for_best_criteria)
        qb = qb.add_from(self.config['settings']['similar_users_groups']['hbase_table_dist'], 'd')
        qb = qb.add_insert(table=table_similars_for_best_criteria)
        qb = qb.add_select('d.key.month,\
                            d.key.criteria,\
                            d.key.groupCriteria,\
                            d.average,\
                            d.penalty,\
                            d.numberCustomers,\
                            d.coefficientVariation,\
                            d.coefficientDispersion')
        qb = qb.add_where('d.key.companyId = {} AND\
                           d.key.type = "{}" '.format(companyId, mainEnergyType))
        try:
            if similar_users_group_calculation:
                qb.execute_query()
        except Exception as e:
            raise Exception('Failed in executing query {}'.format(e))

        # SIMILAR USERS GROUPS
        # 13.6. [Python Map Reduce] Detect the best similar criteria for each modellingUnitId
        try:
            if similar_users_group_calculation:
                call(["hadoop", "fs", "-rm", "-r", location_similar_results + '_best_criteria'])
                self.similars_best_criteria(
                    input_path=location_joined,
                    input_similars_path=location_similar_results + '_bc',
                    output_path=location_similar_results + '_best_criteria',
                    columns=fields,
                    columns_similars=[item[0] for item in fields_similars_bc],
                    criteria=criteria,
                    #TODO: add functions to package
                    min_yearmonths=[
                        int((last_day_n_month(ts_to, 0) + relativedelta(seconds=1) - relativedelta(months=i)).strftime("%Y%m")) for
                        i in n_months_for_best_criteria_calc],
                    n_months=n_months_for_best_criteria_calc,
                    fixed_part=fixed_percentage_for_cost,
                    keys_for_filtering_and_best_criteria=['rawMonths', 'v']
                )
        except Exception as e:
            raise Exception('MRJob errors: {}'.format(e))

        # SIMILAR USERS GROUPS
        # [Python Map Reduce] Load the best similar criteria results to REST and HBase databases
        try:
            if similar_users_group_calculation:
                self.load_to_dbs(
                    input_path=location_similar_results + '_best_criteria',
                    type=mainEnergyType,
                    companyId=companyId,
                    input_fields=['modellingUnitId', 'months_considered', 'fixed_cost_percentage', 'best_criteria',
                                  'best_group_criteria', 'best_criteria_dict', 'difference', 'cost', 'values'],
                    output_hbase_key=['modellingUnitId',
                                      'companyId',
                                      'type',
                                      'months_considered',
                                      'fixed_cost_percentage'],
                    output_hbase_fields=[('c:criteria', 'best_criteria', 'string'),
                                         ('c:groupCriteria', 'best_group_criteria', 'string'),
                                         ('c:results', 'best_criteria_dict', 'string'),
                                         ('c:diffs', 'difference', 'float'),
                                         ('c:costs', 'cost', 'float')],
                    output_mongo_key=[('modellingUnitId', 'modellingUnitId', 'string'),
                                      ('companyId', 'companyId', 'int'),
                                      ('type', 'type', 'string'),
                                      ('numberMonths', 'months_considered', 'int'),
                                      ('fixedCostPerc', 'fixed_cost_percentage', 'int')],
                    output_mongo_fields=[('criteria', 'best_criteria', 'string'),
                                         ('groupCriteria', 'best_group_criteria', 'string'),
                                         ('results', 'best_criteria_dict', 'dict'),
                                         ('diff', 'difference', 'float'),
                                         ('cost', 'cost', 'float'),
                                         ('values', 'values', 'dict')],
                    output_hbase_table=self.config['settings']['similar_users_groups'][
                        'hbase_table_best_criteria'],
                    output_mongo_collection=self.config['settings']['similar_users_groups'][
                        'mongo_collection_best_criteria'],
                    create_hbase_column_family_if_not_exists={'c': dict()},
                    mongo_operation="update"
                )
        except Exception as e:
            raise Exception('Load to DB errors: {}'.format(e))

        self.logger.info('Module EDINET_comparisons execution finished...')



if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ComparisonModule()
    job.run(commandDictionary)


    """
from module_edinet.edinet_comparison.task import ComparisonModule
from datetime import datetime
params = {
    'companyId': 1092915978,
    'companyId_toJoin': [3230658933],
    'type': ['electricityConsumption', 'tertiaryElectricityConsumption'],
    'eff_users': 80,
    'ts_to': datetime(2016, 12, 31, 23, 59, 59),
    'criteria': ['entityId + postalCode + useType', 'entityId + useType']
 }
t = ComparisonModule()
t.run(params) 
    """