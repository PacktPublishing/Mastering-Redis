__author__ = "Jeremy Nelson"

import csv
import datetime
import os
import redis

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

def extract_load(datastore=redis.StrictRedis()):
    for filename in ['cape-town-2014.csv', 
                     'denver-2014.csv',
                     'tokyo-2014.csv']:

        weather_csv = csv.reader(open(os.path.join(
            CURRENT_DIR,
            filename)))
        field_names = next(weather_csv)
        city_date = filename.split(".")[0]
        temp_key = "{}-temp-mean".format(city_date)
        dew_point_key = "{}-dew-point-mean".format(city_date)
        humidity_key = "{}-humidity-mean".format(city_date)
        pressure_key = "{}-pressure-mean".format(city_date)
        precipitation_key = "{}-precipitation".format(city_date)
        for row in weather_csv:
            if len(row) < 8:
                continue
            pipeline = datastore.pipeline(transaction=True)
            date_field = row[0]
            pipeline.hsetnx(temp_key, date_field, row[2])
            pipeline.hsetnx(dew_point_key, date_field, row[4])
            pipeline.hsetnx(humidity_key, date_field, row[8])
            pipeline.hsetnx(pressure_key, date_field, row[11])
            pipeline.hsetnx(precipitation_key, date_field, row[19])
            pipeline.execute()
        print("Finished {}".format(city_date))

def run_regression():
    pass
