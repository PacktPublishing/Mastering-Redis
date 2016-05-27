__author__ = "Jeremy Nelson"
__license__ = "BSD"

import rdflib
import requests

BLAZEGRAPH_URL = 'http://localhost:8080/bigdata/sparql'
FEDORA_URL = 'http://localhost:8080/fedora/rest'
LDFS_URL = 'http://localhost:18150'

def ingest_graph(graph):
    sparql = """SELECT DISTINCT ?subject WHERE { ?subject ?pred ?obj . }"""
    for row in graph.query(sparql):
        subject = row[0]
        fedora_result = requests.post(FEDORA_URL)
        fedora_subject = rdflib.URIRef(fedora_result.text)
        subject_graph = rdflib.Graph()
        subject_graph.parse(str(fedora_subject))
        subject_graph.namespace_manager.bind(
            'schema', 
            'http://schema.org/')
        subject_graph.namespace_manager.bind(
            'owl',
            str(rdflib.OWL))
        subject_graph.add((fedora_subject, rdflib.OWL.sameAs, subject))
        for pred, obj in graph.predicate_objects(
            subject=subject):
            subject_graph.add((fedora_subject, pred, obj))
        print(subject_graph.serialize(format='turtle').decode())
        update_result = requests.put(str(fedora_subject),                
            data=subject_graph.serialize(format='turtle'),
            headers={"Content-Type": "text/turtle"})
        
def populate_ldfs():
    result = requests.post(
        "http://localhost:8080/bigdata/sparql",
        data={"query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o .}",
              "format": "json"})
    bindings = result.json().get('results').get('bindings')
    for row in bindings:
        add_triple_result = requests.post(
            #"{}/triple".format(LDFS_URL), 
            LDFS_URL,
            data={"s": row.get('s').get('value'),
                  "p": row.get('p').get('value'),
                  "o": row.get('o').get('value')})
    
