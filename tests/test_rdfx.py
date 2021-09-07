"""
This Python 3.8 code tests the ``utils`` module.
Beware, these tests cover only some functions and only some scenarios.
Keep adding tests!
CHANGELOG:
- 2021-04-21:   David Habgood (DH): Initial version
"""
import unittest
from src.rdfx import *
from pathlib import Path


class UtilsTestCase(unittest.TestCase):

    def test_make_output_file_path_ttl_to_nt(self):
        output = make_output_file_path(input_file_path=Path('/mnt/sda2/Surround/rdfx/tests/test_data/file_1.ttl'),
                                       input_format='turtle',
                                       output_format='nt',
                                       in_place=False)
        assert output == Path('/mnt/sda2/Surround/rdfx/tests/test_data/file_2.nt')

    def test_make_output_file_path_inplace(self):
        output = make_output_file_path(input_file_path=Path('/mnt/sda2/Surround/rdfx/tests/test_data/file_1.ttl'),
                                       input_format='turtle',
                                       output_format='turtle',
                                       in_place=True)
        assert output == Path('/mnt/sda2/Surround/rdfx/tests/test_data/file_2.ttl')

    def test_make_output_file_path_new_file(self):
        output = make_output_file_path(input_file_path=Path('/mnt/sda2/Surround/rdfx/tests/test_data/file_1.ttl'),
                                       input_format='turtle',
                                       output_format='turtle',
                                       in_place=False)
        assert output == Path('/mnt/sda2/Surround/rdfx/tests/test_data/file_2.new.ttl')

    # def test_merge(self):
    #     files = [str(file) for file in Path('test_data').glob('*')]
    #     merge(files)


if __name__ == "__main__":
    unittest.main()
