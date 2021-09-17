import unittest
import os
from pathlib import Path
from persistence_systems import prepare_files_list

class MyTestCase(unittest.TestCase):
    def test_convert_1_file(self):
        file_1 = Path('../tests/data/merge_02.rdf')
        os.chdir('../rdfx')
        os.system(f"python rdfx.py convert {str(file_1)} nt")
        file_1.with_suffix('.nt').unlink()

    def test_convert_2_file(self):
        file_1 = Path('../tests/data/merge_01.ttl')
        file_2 = Path('../tests/data/merge_02.rdf')
        os.chdir('../rdfx')
        os.system(f"python rdfx.py convert {str(file_1)} {str(file_2)} nt")
        file_1.with_suffix('.nt').unlink()
        file_2.with_suffix('.nt').unlink()

    def test_convert_directory(self):
        dir = Path('../tests/data/')
        to_retain = prepare_files_list(dir)
        os.chdir('../rdfx')
        os.system(f"python rdfx.py convert {dir} nt")
        # remove the files
        all_files = prepare_files_list(dir)
        to_remove = set(all_files)-set(to_retain)
        [file.unlink() for file in to_remove]

    def test_merge_directory(self):
        pass

if __name__ == '__main__':
    unittest.main()
