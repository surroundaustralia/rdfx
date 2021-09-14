import unittest
import rdflib
from moto import mock_s3
from rdflib import Graph
import botocore
from rdfx.persistence_systems import S3

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle"
)

class S3Tests(unittest.TestCase):

    @mock_s3
    def test_persist(self):

        s3_ps = S3(bucket='cadastre-3d-semantic-documents',
                   key='test_key',
                   aws_key='test_aws_key',
                   aws_secret='test_aws_secret',
                   rdf_format='ttl'
                   )
        response = s3_ps.persist(g)
        self.assertEqual(response, 'test_key')

    @mock_s3
    def test_missing_credentials(self):
        with self.assertRaises(TypeError):
            s3_ps = S3(bucket='cadastre-3d-semantic-documents',
                       key='test_key',
                       rdf_format='ttl'
                       )
            response = s3_ps.persist(g)
            self.assertEqual(response, 'test_key')

    @mock_s3
    def test_invalid_format(self):
        with self.assertRaises(rdflib.plugin.PluginException):
            s3_ps = S3(bucket='cadastre-3d-semantic-documents',
                       key='test_key',
                       aws_key='test_aws_key',
                       aws_secret='test_aws_secret',
                       rdf_format='blahblah'
                       )
            response = s3_ps.persist(g)
            self.assertEqual(response, 'test_key')

    @mock_s3
    def test_invalid_bucket_name(self):
        with self.assertRaises(botocore.exceptions.ParamValidationError):
            s3_ps = S3(bucket='s3://cadastre-3d-semantic-documents',
                       key='test_key',
                       aws_key='test_aws_key',
                       aws_secret='test_aws_secret',
                       rdf_format='ttl'
                       )
            response = s3_ps.persist(g)
            self.assertEqual(response, 'test_key')
