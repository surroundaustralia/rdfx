"""
This Python 3.8 code tests the generic functions in ``rdfx/persistence_systems.py``
Beware, these tests cover only some functions and only some scenarios.
Keep adding tests!
CHANGELOG:
- 2021-09-09:   David Habgood (DH): Initial version
"""
import logging
from typing import get_args
logging.basicConfig(level=logging.INFO)
import unittest
from rdfx.persistence_systems import *

g = Graph().parse('data/merge_01.ttl')

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

class GenericTests(unittest.TestCase):

    def test_generate_string_ttl(self):
        generated_string = generate_string(g, rdf_format="turtle", leading_comments=None)
        self.assertEqual(reference_string_1, generated_string)

    def test_generate_string_ttl_with_comment(self, rdf_format="turtle", leading_comments=None):
        generated_string = generate_string(g, rdf_format="turtle", leading_comments=[ref_comment_1])
        self.assertEqual(reference_string_2, generated_string)

    def test_generate_string_ttl_with_two_comments(self):
        generated_string = generate_string(g, rdf_format="turtle", leading_comments=[ref_comment_1, ref_comment_2])
        self.assertEqual(reference_string_3, generated_string)

    def test_valid_types(self):
        # this test is (almost) redundant as it reads from the list of valid RDF formats
        allowed_types = get_args(RDF_FORMATS)
        for a_type in allowed_types:
            try:
                File('.', a_type)
            except ValueError:
                self.fail(f"Should not get a ValueError for {a_type}")

    def test_invalid_types(self):
        invalid_type = 'ttl'
        with self.assertRaises(ValueError):
            File('.', invalid_type)

    def test_prepare_files_list_from_string(self):
        output = prepare_files_list('data/merge_01.ttl')
        assert output == [Path('data/merge_01.ttl')]

    def test_prepare_files_list_from_path(self):
        output_path = Path('data/merge_01.ttl')
        output = prepare_files_list('data/merge_01.ttl')
        assert output == [output_path]

    def test_prepare_files_list_from_dir_str(self):
        expected_output = [Path('data/merge_01.ttl'), Path('data/merge_03.json-ld'), Path('data/merge_02.rdf')]
        output = prepare_files_list('data')
        assert output == expected_output

    def test_prepare_files_list_from_dir_path(self):
        expected_output = [Path('data/merge_01.ttl'), Path('data/merge_03.json-ld'), Path('data/merge_02.rdf')]
        output = prepare_files_list(Path('data'))
        assert output == expected_output
