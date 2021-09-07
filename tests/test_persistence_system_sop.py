from rdflib import Graph
import os
from sys import path
path.append("..")
from rdfx.persistence_systems import SOP
from rdfx.persistence_systems import PersistenceSystem

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle"
)

sop = SOP(
    os.getenv("SOP_SYSTEM_IRI"),
    "http://example.com/testgraph",
    os.getenv("SOP_USERNAME"),
    os.getenv("SOP_PASSWORD"),
)
print(issubclass(type(sop), PersistenceSystem))

# print(sop.persist(g).status_code)

"""
# possible test 

SELECT DISTINCT ?g
WHERE {
  GRAPH ?g {
    ?s ?p ?o 
  }
  
  FILTER(CONTAINS(STR(?g), "testgraph"))
}



INSERT DATA {
  GRAPH <http://example.com/testgraph> {
    <a:> <b:> <c:> .
    <a:> <d:> <e:> . 
  }
}

"""
