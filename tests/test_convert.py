import unittest
from rdfx.rdfx import convert
from rdfx.persistence_systems import File
from pathlib import Path
from io import StringIO, BytesIO


class MyTestCase(unittest.TestCase):
    def test_ttl_nt(self):
        expected_output = """<http://orcid.org/0000-0002-8742-7730> <https://schema.org/email> "nicholas.car@surroundaustralia.com"^^<http://www.w3.org/2001/XMLSchema#anyURI> .
<http://orcid.org/0000-0002-8742-7730> <https://schema.org/affiliation> <https://surroundaustralia.com> .
<http://orcid.org/0000-0002-8742-7730> <https://schema.org/jobTitle> "Data Systems Architect" .
<http://orcid.org/0000-0002-8742-7730> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Person> .
<http://orcid.org/0000-0002-8742-7730> <https://schema.org/name> "Nicholas J. Car" .
<http://orcid.org/0000-0002-8742-7730> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#NamedIndividual> .

"""
        input_file = Path("data/file_01.ttl")
        output_format = "nt"
        output_file = Path(f"./converted.{output_format}")
        ps = File(output_file, output_format)
        convert(input_file, ps)
        # nt is unordered so must use readlines to compare
        output_lines = output_file.open().readlines()
        reference_lines = StringIO(expected_output).readlines()
        for line in output_lines:
            assert line in reference_lines
        # delete the file
        output_file.unlink()

    def test_ttl_comments(self):
        expected_output = """# test comment one
# test comment two

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
        input_file = Path("data/file_01.ttl")
        output_format = "turtle"
        output_file = Path(f"./converted.{output_format}")
        leading_comments = ["test comment one", "test comment two"]
        ps = File(output_file, output_format, leading_comments)
        convert(input_file, ps)
        assert expected_output == output_file.read_text()
        # delete the file
        output_file.unlink()

    def test_dir_conversion(self):
        input_file = Path("data")
        for file in input_file.glob("*"):
            output_format = "turtle"
            output_file = Path(f"./converted.{output_format}")
            leading_comments = ["test comment one", "test comment two"]
            ps = File(output_file, output_format, leading_comments)
            convert(input_file, ps)
            assert expected_output == output_file.read_text()
        # delete the file
        output_file.unlink()


if __name__ == "__main__":
    unittest.main()
