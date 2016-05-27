__author__ = "Jeremy Nelson"
__license__ = "Apache 2"

import os
import random
from rediscluster import StrictRedisCluster

AREA_CODE_CLUSTER = StrictRedisCluster(
    startup_nodes=[{"host": "localhost", "port": 9001}])

def assign_codes_to_partitions(
    filename,
    datastore):
    with open(filename) as area_codes_file:
        area_codes = area_codes_file.readlines()
    area_code_shard1 = "area_code:partition:1"
    area_code_shard2 = "area_code:partition:2"
    area_code_shard3 = "area_code:partition:3"
    for i, row in enumerate(area_codes):
        code, geo_name = row.split("\t")
        if i < 106:
            slot= area_code_shard1
        elif i >= 106 and i < 212:
            slot = area_code_shard2
        else:
            slot = area_code_shard3
        datastore.hset(slot, code, geo_name.strip())

def random_phonenumber(area_code):
    number = str(area_code)
    for i in range(7):
        number += "{}".format(random.randint(0,9))
    return number


def area_code_dict(filepath):
    with open(filepath) as fo:
        lines = fo.readlines()
    area_codes = dict()
    for row in lines:
        fields = row.split("\t")
        area_codes[int(fields[0])] = fields[1].strip()
    return area_codes

def populate_cluster(total):
    area_codes = area_code_dict(
        os.path.join(os.path.abspath(os.path.dirname(__file__)),
                     'area-codes.txt'))
    codes = list(area_codes.keys())
    for i in range(total):
        number = random.randint(0, len(codes)-1)
        area_code = codes[number]
        phone_number = random_phonenumber(area_code)
        AREA_CODE_CLUSTER.hsetnx(phone_number, "geographicArea", area_codes[area_code])


