from pathlib import Path

from src.persistence_systems import File
from src.rdfx import merge


def test_merge_directory():
    output_format = "ttl"
    input_dir = Path(Path(__file__).parent / "data")
    files = [file for file in input_dir.glob("*.*")]
    output_file = input_dir / f"merged.{output_format}"
    ps = File(input_dir)
    merge(files, ps, output_format)
    assert output_file.exists()
    # delete the file
    output_file.unlink()
