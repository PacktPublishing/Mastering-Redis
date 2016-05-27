"""marc_example.py

MARC21 Example of storing JSON serialized MARC21 documents into MongoDB and 
using Redis for analytics. 

To use:
    >>> import marc_example
    >>> marc_example.run()
"""
__author__ = "Jeremy Nelson"
__license__ = "Apache2"

import datetime
import json
import pymarc
import random
import redis

from pymongo import MongoClient

MARC_REDIS = redis.StrictRedis()
MONGO_CLIENT = MongoClient()
MARC_DATABASE = MONGO_CLIENT.marc_database
MARC_USAGE = MONGO_CLIENT.marc_usage

def process_records(records):
    """Function takes a list of records, iterates through 
   
    Args:
        records -- List of MARC21 records
    """
    marc_collection = MARC_USAGE.marc_collection 
    start = datetime.datetime.utcnow()
    print("Started processing records {} at {}".format(
        start.isoformat(),
        len(records)))
    for i, record in enumerate(records):
        mongo_id = marc_collection.insert_one(
            json.loads(record.as_json())).inserted_id
        redis_offset = MARC_REDIS.incr("insertion-offset")
        MARC_REDIS.zadd("marc-insertion", redis_offset, str(mongo_id))
        if not i%10 and i > 0:
            print(".", end="")
        if not i%100:
            print(i, end="")
    end = datetime.datetime.utcnow()
    print("Finished processing records at {}, total time={} seconds".format(
        end.isoformat(),
        (end-start).seconds))

def add_mongo_daily_usage(object_id, date):
    usage_collection = MARC_USAGE.usage_collection
    usage_document = { "datetime": date.isoformat(),
                       "marc-id": str(object_id) }
    return usage_collection.insert_one(usage_document).inserted_id

def add_redis_daily_usage(offset, date):
    usage_key = date.strftime("%Y-%m-%d")
    MARC_REDIS.setbit(usage_key, offset, 1)
    
def run_usage_simulation(seed_seconds, runs=90):
    seconds_in_day = 60*60*24
    max_records = int(MARC_REDIS.get('insertion-offset'))
    start = datetime.datetime.utcnow()
    print("Started usage simulation run at {}".format(start.isoformat()))
    for day in range(runs):
        timestamp = datetime.datetime.utcfromtimestamp(
            seconds_in_day*day + seed_seconds)
        daily_usage = random.randint(500, 1000)
        offsets = random.sample(range(1, max_records), daily_usage)
        for offset in offsets:
            result = MARC_REDIS.zrange('marc-insertion', offset, offset)
            if len(result) < 1:
                continue
            object_id = result[0]
            add_mongo_daily_usage(object_id, timestamp)
            add_redis_daily_usage(offset, timestamp)
            
