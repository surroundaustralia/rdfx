from pathlib import Path

from rdfx.persistence_systems import File
from rdfx.rdfx_cli import merge


def test_merge_directory():
    output_format = "ttl"
    input_dir = Path(Path(__file__).parent / "data")
    files = [file for file in input_dir.glob("*.*")]
    output_filename = "output_filename"
    output_file = input_dir / f"output_filename.{output_format}"
    ps = File(input_dir)
    merge(files, ps, output_format, output_filename)
    assert output_file.exists()
    # delete the file
    output_file.unlink()
