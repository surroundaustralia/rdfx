import botocore
import pytest
import rdflib
from moto import mock_s3
from rdflib import Graph

from src.persistence_systems import S3

g = Graph().parse(
    data="""
    <a:> <b:> <c:> .
    <a:> <d:> <e:> .
    """,
    format="turtle",
)


@mock_s3
def test_persist():
    s3_ps = S3(
        bucket="cadastre-3d-semantic-documents",
        aws_key="test_aws_key",
        aws_secret="test_aws_secret",
    )
    response = s3_ps.persist(g, filename="test_key", rdf_format="ttl")
    assert response == "test_key"


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
