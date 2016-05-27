"""Redis Memory Encoding Tests"""
__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import matplotlib.pyplot as plt
import random
import redis
import uuid

INSTANCE1 = redis.StrictRedis()
INSTANCE2 = redis.StrictRedis(port=6380)

def size(instance, key):
    debug = instance.debug_object(key)
    return debug.get("serializedlength")

def reset():
    INSTANCE1.flushall()
    INSTANCE2.flushall()

def plot_hashes(runs=500):
    reset()
    key = "test-hash"
    INSTANCE2.config_set('hash-max-ziplist-entries', 0)
    run, zip_list, hash_map = [], [], []
    for i in range(runs):
        field = "f{}".format(i)
        INSTANCE1.hset(key, field, i)
        INSTANCE2.hset(key, field, i)
        zip_list.append(size(INSTANCE1, key))
        hash_map.append(size(INSTANCE2, key))
    l1, l2 = plt.plot(zip_list, 'r--', hash_map, 'g-')
    plt.title("Redis Hash Encodings Ziplist vs. Hashtable")
    plt.xlabel("Number of Fields in Hash")
    plt.ylabel("Size of Hash in Bytes")
    plt.legend((l1, l2), ('ziplist', 'hash-table'), loc='lower right')
    plt.show()


def plot_list(runs=512):
    reset()
    # Force all lists to be linkedlists
    INSTANCE2.config_set("list-max-ziplist-entries", 0) 
    key = "test-list"
    run, zip_list, linked_list = [], [], []
    for i in range(runs):
        run.append(i)
        uid = uuid.uuid4()
        INSTANCE1.lpush(key, uid)
        INSTANCE2.lpush(key, uid)
        zip_list.append(size(INSTANCE1, key))
        linked_list.append(size(INSTANCE2, key))
    l1, l2 = plt.plot(zip_list, 'r--', linked_list, 'g-')
    title = "Redis List Encoding "
    if runs < 100:
        title += "Very Small Lists"
    else:
        title += "Ziplist vs. Linked List"
    plt.title(title)
    plt.xlabel("Number of UUIDs")
    plt.ylabel("Serialized Lengths in Bytes")
    plt.legend((l1, l2), ('ziplist', 'linked-list'), loc='lower right')
    plt.show()

def plot_set(runs=512):
    reset()
    INSTANCE2.config_set("set-max-intset-entries", 0)
    key, intset, normal = "test-set", [], []
    for i in range(runs):
        INSTANCE1.sadd(key, i)
        INSTANCE2.sadd(key, i)
        intset.append(size(INSTANCE1, key))
        normal.append(size(INSTANCE2, key))
    i1, n1 = plt.plot(intset, 'r--', normal, 'g-')
    plt.title("Redis Set Encoding Comparison for Integer Sets")
    plt.xlabel("Set Members")
    plt.ylabel("Serialized Length in Bytes")
    plt.legend((i1, n1), ('intset', 'hashtable'), loc="lower right")
    plt.show()

def plot_sortedset(runs=128):
    reset()
    INSTANCE2.config_set('zset-max-ziplist-entries', 0)
    key, ziplist, default = "test-sorted-set", [], []
    for i in range(runs):
        uid = uuid.uuid4()
        INSTANCE1.zadd(key, 0, uid)
        INSTANCE2.zadd(key, 0, uid)
        ziplist.append(size(INSTANCE1, key))
        default.append(size(INSTANCE2, key))
    z1, d1 = plt.plot(ziplist,'r--', default, 'g-')
    plt.title("Redis Sorted Set Encoding Comparison")
    plt.xlabel("Sorted Set UUID Members")
    plt.ylabel("Serialized Length in Bytes")
    plt.legend((z1, d1), ('skiplist', 'hashtable'), loc="lower right")
    plt.show()
   
def populate_tea(full=True):
    for i in range(10000):
        if random.random() <= .6:
            member = i
            if full:
                member = "tea:{}".format(i) 
            INSTANCE1.sadd("teas:caffeinated", member)
            INSTANCE1.setbit("teas:caffeine", i, 1)

         
