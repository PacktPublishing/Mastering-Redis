"""
  marc_hash - implements a Redis hash value store for MARC21 records using the 
  technique described in the section: Using hashes to abstract a very memory 
  efficient plain key-value store on top of Redis at 
  http://www.redis.io/topics/memory-optimization
"""
__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import pymarc
import redis

# Global Redis Database
REDIS = redis.StrictRedis()

class InvalidKeyError(Exception):

    def __init__(self, key, condition):
        self.value = "{} invalid because {}".format(key, condition)
    
    def __str__(self):
        return repr(self.value)


def basic_ingestion(record):
    """Function takes a MARC record, converts it into JSON, and then
    saves the result as string in Redis.

    Args:
        record -- MARC21 record
    """
    marc_json = record.as_json()
    redis_key = "marc:{}".format(REDIS.incr("global:marc"))
    REDIS.set(redis_key, marc_json)

def split_key(redis_key):
    """Function takes a Redis Key and splits into a key and field 
    raises InvalidKeyError exception if redis_key does not meet 
    conditions.

    Args:
        redis_key -- MARC record Redis key

    Returns:
        tuple of new key and field
    """
    new_key, field = redis_key[:-2], redis_key[-2:]
    if field.startswith(":"):
        field = field[1:]
    if not new_key.startswith('marc'):
        raise InvalidKeyError(redis_key, "Must start with marc")
    try:
        int(field)
    except ValueError:
        raise InvalidKeyError(redis_key, "Last two characters must be integers")
    return new_key, field 

def hash_ingestion(record):
    """Function takes a MARC record, generates a key that is then split up into 
    base_key and field where the JSON serialization is stored.

    Args:
        record -- MARC Record
    """
    marc_json = record.as_json()
    key, field = split_key("marc:{}".format(REDIS.incr("global:marc")))
    REDIS.hset(key, field, marc_json)
    


