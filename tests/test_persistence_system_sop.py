import json
import os

from rdflib import Graph

from rdfx.persistence_systems import SOP

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle"
)

workflow_graph = "urn:x-evn-tag:wa_licensed_surveyors_act_1909____06_a0_03_:basic_workflow___administrator_on_2021_09_24_16_32_50:Administrator"
queryable_graph = "http://topbraid.org/examples/kennedys"

# TODO complete test once a dev endpoint is available
# def test_sop_persist():
#     sop = SOP(
#         os.getenv("SOP_SYSTEM_IRI", "http://localhost:8083"),
#         os.getenv("SOP_NAMED_GRAPH", workflow_graph),
#         os.getenv("SOP_USERNAME", ""),
#         os.getenv("SOP_PASSWORD", ""),
#     )
#     results = sop.persist(g)

def test_sop_query():
    query = """SELECT * { ?s ?p ?o } LIMIT 10"""
    sop = SOP(
        os.getenv("SOP_SYSTEM_IRI", "http://localhost:8083"),
        os.getenv("SOP_NAMED_GRAPH", queryable_graph),
        os.getenv("SOP_USERNAME", ""),
        os.getenv("SOP_PASSWORD", ""),
    )
    results = json.loads(sop.query(query).text)["results"]["bindings"]
    # simply validating we are getting results back at this point
    assert len(results) == 10
