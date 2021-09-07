from sys import path
path.append("..")
from pathlib import Path
from rdflib.namespace import RDF
from rdflib import Namespace
SDO = Namespace("https://schema.org/")


def test_merge_different_filetypes():
    from rdfx.rdfx import merge

    files = []
    for f in Path(Path(__file__).parent / "data").glob("*.*"):
        files.append(f)

    g = merge(files)
    people = 0
    for s in g.subjects(predicate=RDF.type, object=SDO.Person):
        people += 1

    assert people == 3
