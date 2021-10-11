from pathlib import Path

from rdflib import Graph

from rdfx.persistence_systems import File

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle",
)


def test_pathlib():
    """Test a graph can be persisted to file from a pathlib Path"""
    file_to_create = Path("./file_test.ttl").resolve()
    file_ps = File(".")
    file_ps.persist(g, filename="file_test", rdf_format="ttl")
    assert file_to_create.exists()
    # delete the file
    file_to_create.unlink()


# def test_content():
#     """Check the content written to disk is correct"""
#     #TODO figure out how to check graphs are the "same" or isomorphic with RDFLib
#     file_to_create = Path('./file_test.ttl').resolve()
#     file_ps = File('.')
#     file_ps.persist(g, filename='file_test', rdf_format='ttl')
#
#     g2 = Graph().parse(str(file_to_create))
#     assert g == g2


def test_comment():
    pass


def test_other_formats():
    pass
