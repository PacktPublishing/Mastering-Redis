"""BIBCAT Examples

"""
__author__ = "Jeremy Nelson"

import hashlib
import json
import pymarc
import random
import rdflib
import redis
import socket

from elasticsearch import Elasticsearch

# Digests 
BF_AUTH_PT_DIGEST = 'a548a25005963f85daa1215ad90f7f1a97fbe749'

# SPARQL Statements
SPARQL_PERSON_QUERY = """PREFIX bf: <http://bibframe.org/vocab/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?sub ?pt
WHERE {
  ?sub rdf:type bf:Person .
  ?sub bf:authorizedAccessPoint ?pt
}"""

# Functions

def add_triple(datastore, subject, predicate, object_):
    subject_sha1 = hashlib.sha1(subject.encode()).hexdigest()
    predicate_sha1 = hashlib.sha1(predicate.encode()).hexdigest()
    object_sha1 = hashlib.sha1(object_.encode()).hexdigest()
    transaction = datastore.pipeline(transaction=True)
    transaction.set(subject_sha1, subject)
    transaction.set(predicate_sha1, predicate)
    transaction.set(object_sha1, object_)
    transaction.set("{}:{}:{}".format(
        subject_sha1,
        predicate_sha1,
        object_sha1),
        1)
    transaction.execute()

def check001(record):
    if len(record.get_fields('001')) < 1:
        record.add_field(
            pymarc.Field(tag='001', 
                data=''.join([str(i) for i in random.sample(range(10), 6)])))
    return record

def convert2bibframe(record):
    return xquery_socket(pymarc.record_to_xml(record, namespace=True))

def index_graph(graph, search_index):
    pass

def dedup_bibframe(graph, cache_datastore):
    query = graph.query(SPARQL_PERSON_QUERY)
    for row in query:
        subject = row[0]
        access_point = row[1]
        access_point_digest = hashlib.sha1(str(access_point).encode()).hexdigest()
        pattern = "*:{}:{}".format(BF_AUTH_PT_DIGEST, access_point_digest)
        existing_subjects = cache_datastore.keys(pattern)
        if len(existing_subjects) > 0:
            subject_digest = existing_subjects[0].decode().split(":")[0]
            subject_iri = cache_datastore.get(subject_digest)
            new_subject = rdflib.URIRef(subject_iri.decode())
            for pred, obj in graph.predicate_objects(subject=subject):
                graph.add((new_subject, pred, obj))
                graph.remove((subject, pred, obj))
    return graph

def process_record(record, 
                   cache_datastore=redis.StrictRedis(), 
                   search_index=Elasticsearch()):
    graph = convert2bibframe(record) 
    graph = dedup_bibframe(graph, cache_datastore)
    for s,p,o in graph:
        add_triple(cache_datastore, str(s), str(p), str(o))
    print("Finished processing {}".format(record.title()))
    index_graph(graph, search_index)


def run_dedup_experiment(
        pp_filepath, 
        md_filepath, 
        cache_datastore=redis.StrictRedis(), 
        search_index=Elasticsearch()):
    """Runs experiment for de-duplicating BIBFRAME Person RDF graphs using MARC
    records from two samples representing Pride and Prejudice and Moby Dick
    records. 
    """
    pride_prejudice_records = [check001(r) for r in pymarc.MARCReader(
        open(pp_filepath, "br+"),
        to_unicode=True)]
    moby_dick_records = [check001(r) for r in pymarc.MARCReader(
        open(md_filepath, "br+"), 
        to_unicode=True)]
    for recs in [pride_prejudice_records, moby_dick_records]:
        for record in recs:
            process_record(record, cache_datastore, search_index)
                                               

def xquery_socket(raw_xml):
    """Function takes raw_xml and converts to BIBFRAME RDF

    Args:
       raw_xml -- Raw XML 
    """
    xquery_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    xquery_server.connect(('localhost', 8089))
    xquery_server.sendall(raw_xml + b'\n')
    rdf_xml = b''
    while 1:
        data = xquery_server.recv(1024)
        if not data:
            break
        rdf_xml += data
    xquery_server.close()
    bf_graph = rdflib.Graph()
    for namespace in [("bf", "http://bibframe.org/vocab/"),
                      ("schema", "http://schema.org/")]:
        bf_graph.namespace_manager.bind(namespace[0], namespace[1])
    bf_graph.parse(data=rdf_xml.decode(), format='xml')
    return bf_graph
