import botocore
import pytest
import rdflib
from botocore.exceptions import BotoCoreError
from moto import mock_s3
from rdflib import Graph

from rdfx.persistence_systems import S3

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle",
)


# @mock_s3
# def test_persist():
#     with pytest.raises(BotoCoreError):
#         # TODO find where the NoSuchBucket exception actually is!
#
#         # TODO find why boto converts a string to tuple - doesn't appear to be issue on my end - following code should
#         #  create a bucket (plenty of examples online) - but fails to as it thinks the bucket name being passed is a tuple
#         # bucket = "cadastre-3d-semantic-documents",
#         # aws_key = "test_aws_key",
#         # aws_secret = "test_aws_secret",
#         # region_name = 'ap-southeast-2'
#         # client = boto3.resource('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name=region_name)
#         # client.create_bucket(Bucket="cadastre-3d-semantic-documents")
#
#         s3_ps = S3(
#             bucket="cadastre-3d-semantic-documents",
#             aws_key="test_aws_key",
#             aws_secret="test_aws_secret",
#         )
#         response = s3_ps.persist(sample_graph, filename="test_key", rdf_format="ttl")
#         assert response == "test_key"
#


@mock_s3
def test_missing_credentials():
    with pytest.raises(TypeError):
        s3_ps = S3(bucket="cadastre-3d-semantic-documents")
        s3_ps.persist(g, filename="test_key", rdf_format="ttl")


@mock_s3
def test_invalid_format():
    with pytest.raises(rdflib.plugin.PluginException):
        s3_ps = S3(
            bucket="cadastre-3d-semantic-documents",
            aws_key="test_aws_key",
            aws_secret="test_aws_secret",
        )
        s3_ps.persist(g, filename="test_key", rdf_format="blahblah")


@mock_s3
def test_invalid_bucket_name():
    with pytest.raises(botocore.exceptions.ParamValidationError):
        s3_ps = S3(
            bucket="s3://cadastre-3d-semantic-documents",
            aws_key="test_aws_key",
            aws_secret="test_aws_secret",
        )
        s3_ps.persist(g, filename="test_key", rdf_format="ttl")
