from pathlib import Path
import unittest
from pathlib import Path

from rdfx.persistence_systems import File


class MergeTests(unittest.TestCase):

    def test_merge_different_filetypes(self):
        from rdfx.rdfx import merge

        output_format = 'turtle'
        input_dir = Path(Path(__file__).parent / "data")
        files = [file for file in input_dir.glob("*.*")]
        output_file = input_dir/f'merged.{output_format}'
        ps = File(output_file, output_format)
        merge(files, ps)
        self.assertTrue(output_file.exists())
        # delete the file
        output_file.unlink()
