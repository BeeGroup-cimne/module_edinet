import os
from datetime import datetime
from tempfile import NamedTemporaryFile

from bson import json_util
import json
from module_edinet.module_python2 import BeeModule2
import sys


class ETL_stations(BeeModule2):
    """
    Get Station information from mongo and save it to hbase
    """
    def __init__(self):
        super(ETL_stations, self).__init__("edinet_etl_stations")
        #delete hdfs directory found in config path on finish

    def LatLngToDMS(self, value, type):
        lat_0 = int(value)  # Parte entera con o sin signo
        lat_decimal_0 = abs(value) - abs(lat_0)  # Parte decimal
        lat_1 = int(60 * lat_decimal_0)
        lat_decimal_1 = (60 * lat_decimal_0) - lat_1  # Parte decimal
        lat_2 = int(round(60 * lat_decimal_1))
        if type == 'Lat':
            text = 'N' if lat_0 > 0 else 'S'
        else:
            text = 'E' if lat_0 > 0 else 'W'
        return "{:0>2d} ".format(lat_0) + "{:0>2d} ".format(lat_1) + "{:0>2d} ".format(lat_2) + text

    def save_station_into_hbase(self, table_name, station):
        row = {}
        # prepare documents to iterate them
        try:
            altitude = station.pop('altitude')
            address = station.pop('address')
            GPS = address.pop('GPS')
            companyId = station.pop('companyId')
            dateStart = station.pop('dateStart').strftime('%s') if 'dateStart' in station and isinstance(
                station['dateStart'], datetime) else None
            dateEnd = station.pop('dateEnd').strftime('%s') if 'dateEnd' in station and isinstance(station['dateEnd'],
                                                                                                   datetime) else None

            row_key = str(companyId) + '~' + station.pop('stationId')

            # dataStart
            if dateStart:
                row['address:dateStart'] = str(dateStart.encode('utf-8'))
            # dataEnd
            if dateEnd:
                row['address:dateEnd'] = str(dateEnd.encode('utf-8'))
            # altitude
            row['address:altitude'] = str(altitude.encode('utf-8'))
            # other address
            for key, value in address.iteritems():
                row['address:{}'.format(key)] = str(value.encode('utf-8'))
            # GPS
            # Doesn't matter if the latitude is expressed in lat/latitude or the longitude is expressed in long/longitude
            # Also transform the float latitude and longitude values into the correct string format.
            for key, value in GPS.iteritems():

                if key == 'latitude' or key == 'lat':
                    try:
                        float(value)
                        row['address:lat'] = str(self.LatLngToDMS(value, 'Lat').encode('utf-8'))
                    except ValueError:
                        row['address:lat'] = str(value.encode('utf-8'))

                elif key == 'longitude' or key == 'long':
                    try:
                        float(value)
                        row['address:long'] = str(self.LatLngToDMS(value, 'Long').encode('utf-8'))
                    except ValueError:
                        row['address:long'] = str(value.encode('utf-8'))

            table = self.hbase.table(table_name)
            table.put(row_key, row)

        except KeyError as e:
            self.logger.debug('Station with missing fields: {}. Not saved into HBase'.format(e))

    def module_task(self, params):
        self.logger.info('Starting Stations ETL')
        """CHECK INCONSISTENCIES IN params"""
        try:
            last_etl = params['last_etl'] if 'last_etl' in params else None
        except KeyError as e:
            raise Exception('Not enough parameters provided to module: {}'.format(e))

        query = {}
        if last_etl:
            query = {
                '_updated': {
                    '$gte': last_etl
                }
            }
            # setting variables for readability
        collection = self.config['mongodb']['collection']

        self.logger.debug('Querying for stations in MongoDB: {}'.format(query))

        projection = {
            '_id': 0,
            '_created': 0,
            '_updated': 0
        }

        table_name = self.config['hbase_table']['name']

        for station in self.mongo[collection].find(query, projection):
            self.save_station_into_hbase(table_name, station)

if __name__ == "__main__":
    commandDictionary = json.loads(sys.argv[1], object_hook=json_util.object_hook)
    job = ETL_stations()
    job.run(commandDictionary)


    """
from module_edinet.edinet_baseline.task import ETL_stations
from datetime import datetime
params = {}
t = ETL_stations()
t.run(params) 
    """