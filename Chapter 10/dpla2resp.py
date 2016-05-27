__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import hashlib
import json
import rdflib

DC = rdflib.Namespace("http://purl.org/dc/elements/1.1/")
EDM = rdflib.Namespace("http://www.europeana.eu/schemas/edm/")

def generate_redis_protocol(cmd):
    proto = ""
    proto += "*" + str(len(cmd)) + "\r\n"
    for arg in cmd:
        proto += "$" + str(len(arg)) + "\r\n"
        proto += arg + "\r\n"
    return proto


def aggregation2resp(record):
    raw_protocol = ''
    record_iri = record.pop('id')
    record_hash = hashlib.sha1(record_iri.encode())
    record_type = record.pop("ingestType")
    rdf_type_hash = hashlib.sha1(str(rdflib.RDF.type).encode())
    record_type_hash = hashlib.sha1(record_type.encode()) 
    raw_protocol += generate_redis_protocol(
        ["MSETNX",
         record_hash.hexdigest(),
         record_iri,
         rdf_type_hash.hexdigest(),
         str(rdflib.RDF.type),
         record_type_hash.hexdigest(),
         record_type])
    record_pred_obj = "{}:pred-obj".format(record_hash.hexdigest())
    raw_protocol += generate_redis_protocol(
        ["SADD",
         record_pred_obj,
         "{}:{}".format(rdf_type_hash, record_type_hash)])
    for key in record.keys():
        # EDM simple triples
        if key in ['isShownAt', 
                   'dataProvider', 
                   'aggregatedCHO',
                   'object']:
            key_iri = getattr(EDM, key)
            key_hash = hashlib.sha1(key_iri.encode())
            
            key_value = record.get(key)
            key_value_hash = hashlib.sha1(key_value.encode())
            raw_protocol += generate_redis_protocol(
                ["MSETNX",
                 key_hash.hexdigest(),
                 key_iri,
                 key_value_hash.hexdigest(),
                 key_value])
            raw_protocol += generate_redis_protocol(
                ["SADD",
                 record_pred_obj,
                 "{}:{}".format(key_hash.hexdigest(), 
                                key_value_hash.hexdigest())])
            edm_subj_obj = "{}:subj-obj".format(key_hash.hexdigest())
            raw_protocol += generate_redis_protocol(
                ["SADD",
                 edm_subj_obj,
                 "{}:{}".format(record_hash.hexdigest(),
                                key_value_hash.hexdigest())])
            edm_subj_pred = "{}:subj-pred".format(
                key_value_hash.hexdigest())
            raw_protocol += generate_redis_protocol(
                ["SADD",
                 edm_subj_pred,
                 "{}:{}".format(record_hash.hexdigest(), 
                                key_hash.hexdigest())])
    if '_rev' in record:
        dc_rev_hash = hashlib.sha1(getattr(DC, '_rev').encode())
        dc_rev_value = record.get('_rev')
        dc_rev_value_hash = hashlib.sha1(dc_rev_value.encode())
        raw_protocol += generate_redis_protocol(
            ["MSETNX",
             dc_rev_hash.hexdigest(),
             getattr(DC, '_rev'),
             dc_rev_value_hash.hexdigest(),
             dc_rev_value])
        raw_protocol += generate_redis_protocol(
            ["SADD",
             record_pred_obj,
             "{}:{}".format(dc_rev_hash.hexdigest(),
                            dc_rev_value_hash.hexdigest())])
        raw_protocol += generate_redis_protocol(
            ["SADD",
             "{}:subj-obj".format(dc_rev_hash.hexdigest()),
             "{}:{}".format(record_hash.hexdigest(),
                            dc_rev_value_hash.hexdigest())])
        raw_protocol += generate_redis_protocol(
            ["SADD",
             "{}:subj-pred".format(dc_rev_value_hash.hexdigest()),
             "{}:{}".format(record_hash.hexdigest(),
                            dc_rev_hash.hexdigest())])




    return raw_protocol, record_hash


def dpla2resp(record):
    """Function takes a DPLA JSON-LD Record and converts RDF triples into
    SHA1 keys.
 
    Args:
        record -- JSON-LD DP.LA Record
    """
    record_resp, aggregation_hash = aggregation2resp(record)
    
                        
def provider2resp(provider):
    pass

def sourceResource2resp(source_resource):
    pass
