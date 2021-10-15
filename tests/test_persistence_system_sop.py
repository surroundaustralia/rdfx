import json
import os
import uuid

from rdflib import Graph

from src.persistence_systems import SOP

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle",
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

remote_sop_ps = SOP(
    os.getenv("SOP_SYSTEM_IRI", "http://localhost:8083"),
    os.getenv("SOP_USERNAME", ""),
    os.getenv("SOP_PASSWORD", ""),
)

local_sop_ps = SOP("http://localhost:8083")
existing_local_master_graph = "urn:x-evn-master:new_test_datagraph"
# new_local_workflow = f"{uuid.uuid4()}:Administrator"
# new_local_workflow = f"pineapple:Administrator"

# def test_sop_query():
#     query = "SELECT * { ?s ?p ?o } LIMIT 10"
#     results = json.loads(remote_sop_ps.query(query, queryable_graph).text)["results"][
#         "bindings"
#     ]
#     # simply validating we are getting results back at this point
#     assert len(results) == 10
#
#
# def test_sop_persist():
#     insert_response = local_sop_ps.persist(g, existing_local_master_graph)
#     assert insert_response.reason == "OK"


def test_sop_datagraph_creation():
    response = local_sop_ps.create_datagraph()
    created_workflow_iri = f"urn:x-evn-master:{response}"

    # query - confirm an "empty" (8 default triples only) datagraph has been created
    query = f"""SELECT (COUNT(*) as ?count) WHERE {{GRAPH <{created_workflow_iri}> {{?s ?p ?o}} }}"""
    query_response = local_sop_ps.query(query, created_workflow_iri)
    response_dict = json.loads(query_response.text)
    assert response_dict["results"]["bindings"][0]["count"]["value"] == "8"


def test_sop_workflow_creation():
    workflow_graph_iri = local_sop_ps.create_workflow(
        graph_iri=existing_local_master_graph
    )
    # method should raise any exceptions prior to getting a response
    assert workflow_graph_iri

    # try to insert something in to the graph
    insert_response = local_sop_ps.persist(g, workflow_graph_iri)
    assert insert_response.reason == "OK"

    # check the triples have been inserted in to the new workflow
    query = f"""
    SELECT (COUNT(*) as ?count)
        WHERE {{
          GRAPH <{workflow_graph_iri}>
          {{ ?s ?p ?o . }}
          FILTER NOT EXISTS {{ GRAPH <{existing_local_master_graph}> {{?s ?p ?o}} }}
        }}"""
    query_response = local_sop_ps.query(query, workflow_graph_iri)
    response_dict = json.loads(query_response.text)
    assert response_dict["results"]["bindings"][0]["count"]["value"] == "2"
