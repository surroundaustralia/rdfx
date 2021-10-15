"""
This Python 3.8 code tests the generic functions in ``src/persistence_systems.py``
Beware, these tests cover only some functions and only some scenarios.
Keep adding tests!
CHANGELOG:
- 2021-09-09:   David Habgood (DH): Initial version
"""
import logging
from typing import get_args

import pytest
from rdflib.plugin import PluginException

logging.basicConfig(level=logging.INFO)

from src.persistence_systems import *

g = Graph().parse("tests/data/file_01.ttl")

reference_string_1 = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix sdo: <https://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://orcid.org/0000-0002-8742-7730> a owl:NamedIndividual,
        sdo:Person ;
    sdo:affiliation <https://surroundaustralia.com> ;
    sdo:email "nicholas.car@surroundaustralia.com"^^xsd:anyURI ;
    sdo:jobTitle "Data Systems Architect" ;
    sdo:name "Nicholas J. Car" .

"""
reference_string_2 = """# baseURI: https://data.surroundaustralia.com/manifest/3dcaddocs

@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix sdo: <https://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://orcid.org/0000-0002-8742-7730> a owl:NamedIndividual,
        sdo:Person ;
    sdo:affiliation <https://surroundaustralia.com> ;
    sdo:email "nicholas.car@surroundaustralia.com"^^xsd:anyURI ;
    sdo:jobTitle "Data Systems Architect" ;
    sdo:name "Nicholas J. Car" .

"""
reference_string_3 = """# baseURI: https://data.surroundaustralia.com/manifest/3dcaddocs
# imports: https://data.surroundaustralia.com/manifest/doc

@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix sdo: <https://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://orcid.org/0000-0002-8742-7730> a owl:NamedIndividual,
        sdo:Person ;
    sdo:affiliation <https://surroundaustralia.com> ;
    sdo:email "nicholas.car@surroundaustralia.com"^^xsd:anyURI ;
    sdo:jobTitle "Data Systems Architect" ;
    sdo:name "Nicholas J. Car" .

"""
ref_comment_1 = "baseURI: https://data.surroundaustralia.com/manifest/3dcaddocs"
ref_comment_2 = "imports: https://data.surroundaustralia.com/manifest/doc"


def test_generate_string_ttl():
    generated_string = PersistenceSystem.generate_string(
        g, rdf_format="turtle", leading_comments=None
    )
    assert reference_string_1 == generated_string


def test_generate_string_ttl_with_comment():
    generated_string = PersistenceSystem.generate_string(
        g, rdf_format="turtle", leading_comments=[ref_comment_1]
    )
    assert reference_string_2 == generated_string


def test_generate_string_ttl_with_two_comments():
    generated_string = PersistenceSystem.generate_string(
        g, rdf_format="turtle", leading_comments=[ref_comment_1, ref_comment_2]
    )
    assert reference_string_3 == generated_string


def test_valid_types():
    # this test is (almost) redundant as it reads from the list of valid RDF formats
    allowed_types = get_args(RDF_FORMATS)
    for a_type in allowed_types:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter(
                    "ignore"
                )  # ignore the rdflib NT serializer warning
                String().persist(Graph(), rdf_format=a_type)
        except ValueError:
            raise


def test_invalid_types():
    invalid_type = "aslkdjfsadf"
    with pytest.raises(PluginException):
        File(".").persist(Graph(), "my_graph", rdf_format=invalid_type)


def test_prepare_files_list_from_string():
    output = prepare_files_list("tests/data/file_01.ttl")
    assert output == [Path("tests/data/file_01.ttl")]


def test_prepare_files_list_from_path():
    output_path = Path("tests/data/file_01.ttl")
    output = prepare_files_list("tests/data/file_01.ttl")
    assert output == [output_path]


def test_prepare_files_list_from_dir_str():
    expected_output = [
        Path("tests/data/file_01.ttl"),
        Path("tests/data/file_03.json-ld"),
        Path("tests/data/file_02.rdf"),
    ]
    output = prepare_files_list("tests/data")
    assert output == expected_output


def test_prepare_files_list_from_dir_path():
    expected_output = [
        Path("tests/data/file_01.ttl"),
        Path("tests/data/file_03.json-ld"),
        Path("tests/data/file_02.rdf"),
    ]
    output = prepare_files_list(Path("tests/data"))
    assert output == expected_output
