import json
import os
import uuid

from rdflib import Graph

from rdfx.persistence_systems import SOP

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
    created_workflow_iri = local_sop_ps.create_datagraph()

    # query - confirm an "empty" (8 default triples only) datagraph has been created
    query = f"""SELECT (COUNT(*) as ?count) WHERE {{GRAPH <{created_workflow_iri}> {{?s ?p ?o}} }}"""
    query_response = local_sop_ps.query(query, created_workflow_iri)
    response_dict = json.loads(query_response.text)
    assert response_dict["results"]["bindings"][0]["count"]["value"] == "8"


def test_sop_datagraph_exists():
    graph_to_create = f"datagraph-{uuid.uuid4()}".replace("-", "_")
    not_created_graph = f"datagraph-{uuid.uuid4()}".replace("-", "_")
    graph_to_create_name = local_sop_ps.create_datagraph(datagraph_name=graph_to_create)
    assert local_sop_ps.asset_exists(graph_to_create_name)
    assert not local_sop_ps.asset_exists(not_created_graph)


def test_sop_workflow_exists():
    workflow_to_create = f"workflow-{uuid.uuid4()}".replace("-", "_")
    assert not local_sop_ps.asset_exists(workflow_to_create)
    new_datagraph = local_sop_ps.create_datagraph()
    workflow_name = local_sop_ps.create_workflow(
        graph_iri=new_datagraph, workflow_name=workflow_to_create
    )
    assert local_sop_ps.asset_exists(workflow_name)


def test_sop_duplicate_datagraph_creation():
    import uuid

    new_graph_name = f"datagraph-{uuid.uuid4()}".replace("-", "_")
    response_1 = local_sop_ps.create_datagraph(datagraph_name=new_graph_name)
    response_2 = local_sop_ps.create_datagraph(datagraph_name=new_graph_name)
    assert response_1 != response_2


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
