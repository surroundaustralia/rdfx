import os
from pathlib import Path
from rdfx.persistence_systems import prepare_files_list

def test_convert_1_file():
    file_1 = Path(__file__).parent / "data" / "file_02.rdf"
    output_format = 'xml'
    os.chdir('../rdfx')
    print(file_1.parent)
    os.system(f"python rdfx.py convert -f {output_format} -o {str(file_1.parent)} {str(file_1)} ")
    file_1.with_suffix(f'.{output_format}').unlink()

def test_convert_2_file():
    file_1 = Path(__file__).parent / 'data' / 'file_01.ttl'
    file_2 = Path(__file__).parent / 'data' / 'file_02.rdf'
    output_format = 'xml'
    os.chdir('../rdfx')
    os.system(f"python rdfx.py convert -f {output_format} -o {str(file_1.parent)} {str(file_1)} {str(file_2)}")
    file_1.with_suffix(f'.{output_format}').unlink()
    file_2.with_suffix(f'.{output_format}').unlink()

def test_convert_directory():
    dir = Path(__file__).parent / 'data'
    to_retain = prepare_files_list(dir)
    os.chdir('../rdfx')
    os.system(f"python rdfx.py convert -f xml -o {dir} {dir}")
    # remove the files
    all_files = prepare_files_list(dir)
    to_remove = set(all_files)-set(to_retain)
    print(f"Converted directory of files, creating: {', '.join([str(i.name) for i in to_remove])}")
    [file.unlink() for file in to_remove]

def test_merge_directory():
    dir = Path(__file__).parent / 'data'
    to_retain = prepare_files_list(dir)
    os.chdir('../rdfx')
    os.system(f"python rdfx.py merge -f xml -o {dir }{dir}")
    # remove the files
    all_files = prepare_files_list(dir)
    to_remove = set(all_files)-set(to_retain)
    print(f"Merged directory of files, creating: {to_remove}")
    [file.unlink() for file in to_remove]
