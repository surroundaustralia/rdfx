import unittest
from pathlib import Path

from rdflib import Graph

from rdfx.persistence_systems import File

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle"
)

class GenericTests(unittest.TestCase):

    def test_pathlib(self):
        """Test a graph can be persisted to file from a pathlib Path"""
        file_to_create = Path('./file_test.ttl').resolve()
        file_ps = File(file_to_create, rdf_format='turtle')
        file_ps.persist(g)
        self.assertTrue(file_to_create.exists())
        # delete the file
        file_to_create.unlink()

    def test_content(self):
        """Check the content written to disk is correct"""
        #TODO figure out how to check graphs are the "same" or isomorphic with RDFLib
        file_to_create = Path('./file_test.ttl').resolve()
        file_ps = File(file_to_create, rdf_format='turtle')
        file_ps.persist(g)

        g2 = Graph().parse(str(file_to_create))
        self.assertEqual(g, g2)

    def test_comment(self):
        pass

    def test_other_formats(self):
        pass


