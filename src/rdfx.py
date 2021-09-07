import sys
from os import path
import glob
import rdflib
import argparse
from pathlib import Path
import json
from typing import Union, List
from rdflib import Graph
import os

RDF_FILE_ENDINGS = {
    "ttl": "turtle",
    "turtle": "turtle",
    "json": "json-ld",
    "json-ld": "json-ld",
    "jsonld": "json-ld",
    "owl": "xml",
    "xml": "xml",
    "rdf": "xml",
    "nt": "nt",
    "n3": "n3"
    }

OUTPUT_FILE_ENDINGS = {
    "ttl": "ttl",
    "turtle": "ttl",
    "xml": "xml",
    "json-ld": "json-ld",
    "nt": "nt",
    "n3": "n3"
    }


def _persist_file(graph: Graph, location: Union[str, Path], named_graph=None, comments=None):
	if comments:

    pass


def _persist_s3(graph: Graph, location: Union[str, Path], named_graph=None, comments=None):
	try:
		import boto3
	except ImportError:
		raise
	client = boto3.client(
		"s3",
		aws_access_key_id=os.getenv("AWS_KEY"),
		aws_secret_access_key=os.getenv("AWS_SECRET"),
		)
	parts = Path(self.filename).parts
	object = client.get_object(Bucket=parts[1], Key="/".join(parts[2:]))
	self.doc = Document(BytesIO(object["Body"].read()))


def _persist_tdb2():
    pass


def _return_string():
    pass


def _return_file_object():
    pass


def _persist_graphdb():
    pass


def _persist_fuseki():
    pass


def _persist_edg():
    pass


valid_methods = {
    'file': _persist_file,
    's3': _persist_s3,
    'tdb2': _persist_tdb2,
    'string': _return_string,
    'fileobject': _return_file_object,
    'graphdb': _persist_graphdb,
    'edg': _persist_edg,
    'fuseki': _persist_fuseki,
    }

def persist(
        graph: Graph,
        methods: Union[dict, str] = {'file', '.'},
        output_format='turtle',
        named_graph=None,
        comments=None,
        ):
    """
    :param graph: an RDFLib Graph to persist
    :param methods: the method(s) to persist the graph. Where only one persistence method of the type file, s3, string,
    or fileobject is used, the type/location can be supplied as a string and the unspecified type/location will be
    inferred. Where other methods or multiple methods are required, the methods must be supplied as a dictionary of the
    form: {type: location}. Where 'string' or 'fileobject' methods are used in a dictionary, the location should be
    supplied as Python's 'None' object.
    :param output_format: The format to persist the graph in, e.g. turtle, nt etc. Defaults to turtle.
    :param named_graph: Optional. Persist the triples to a named graph.
    :param comments: Optional. Add comments to the top of the file. Only supported for output formats: turtle
    :return: For the String and File Object methods, output is returned directly.
    """
    if output_format not in OUTPUT_FILE_ENDINGS.keys():
        raise ValueError(f"Output format \"{output_format}\" is not in available output formats: "
                         f"{list(OUTPUT_FILE_ENDINGS.keys())}")

    # validate single string input and convert to dictionary of form {type: location}
    if isinstance(methods, str):
        if methods.startswith("s3://"):
            methods = {'s3', methods}
        elif methods.startswith("http"):
            raise ValueError(f"If using a SPARQL endpoint you must specify the type of triplestore behind the endpoint."
                             f" E.g. persist(graph, methods={{'edg': {methods}}})")
        elif methods.startswith("string"):
            methods = {'string', None}
        elif methods.startswith("fileobject"):
            methods = {'fileobject', None}
        elif methods.startswith("graphdb") or methods.startswith("edg") or methods.startswith("fuseki"):
            raise ValueError(f"When persisting to a triplestore you must specify the sparql endpoint."
                             f" E.g. persist(graph, methods={{{methods}: <https://sparql_endpoint>}})")
        else:
            methods = {'file', methods}

    # persist for each method
    for method_type, method_location in methods.items():
        if method_type not in valid_methods.keys():
            raise ValueError(f"Persistence method \"{method_type}\" is not recognised.\n"
                             f"Valid persistence methods are: {list(valid_methods.keys())}")
		if comments:
			# add to in memory graph
		else:
			graph_bytes = graph.serialize()

        valid_methods[method_type](graph_bytes, method_location, named_graph, comments)


def merge(
        files: List,
        output_format='ttl',
        output_location: Union[dict, str] = {'file', '.'},
        comments=None,
        ):
    ...
    persist(graph=g, methods=output_location, comments=comments)


def get_input_format(file_path):
    input_format = rdflib.util.guess_format(file_path)
    if input_format is None:
        if sys.argv[1].endswith("json-ld") or file_path.endswith("jsonld") or file_path.endswith("jsonld"):
            input_format = "json-ld"
        else:
            raise Exception("ERROR: Cannot guess the RDF format of input file {}".format(file_path))
    return input_format


def make_output_file_path(input_file_path, input_format, output_format, in_place):
    suffix = ''
    if input_format == output_format and not in_place:
        suffix += ".new"
    suffix += f".{OUTPUT_FILE_ENDINGS.get(output_format)}"
    return input_file_path.with_suffix(suffix)


def convert(input_file_path, input_format, output_file_path, output_format, prefixes=None):
    g = rdflib.Graph().parse(str(input_file_path), format=input_format)
    if prefixes is not None:
        for k, v in prefixes.items():
            g.bind(k, v)
    g.serialize(destination=str(output_file_path), format=output_format)


def process_files(args):
    file_or_dir = Path(args.data)
    if file_or_dir.is_dir():
        files = [file for file in file_or_dir.glob('*')]
        for file in files:
            print(f"Converting file: {file}")
            input_format = get_input_format(file)
            output_file_path = make_output_file_path(file, input_format, RDF_FILE_ENDINGS.get(args.ext), args.inplace)
            convert(file, input_format, output_file_path, args.ext, prefixes)
    # if Path.is_dir(args.data):
    # 	print("converting directory {}".format(sys.argv[1]))
    # 	for f in glob.glob(path.join(sys.argv[1], "*")):
    # 		print("converting {}".format(f))

    elif file_or_dir.is_file():
        pass

    # Path.is_file(args.data):
    # print("converting {}".format(sys.argv[1]))
    #
    # output_file_path = make_output_file_path(
    # 	args.data,
    # 	get_input_format(args.data),
    # 	RDF_FILE_ENDINGS.get(args.ext),
    # 	args.inplace
    # )
    #
    # convert(
    # 	args.data,
    # 	get_input_format(args.data),
    # 	output_file_path,
    # 	RDF_FILE_ENDINGS.get(args.ext),
    # 	prefixes
    # )
    else:
        raise ValueError(
            "The value you supplied for 'data' was {} but that is not a directory or file as required".format(args.data)
            )

    print("converted")


if __name__ == "__main__":

    if "-h" not in sys.argv and len(sys.argv) < 3:
        print("ERROR: You must supply two command line arguments: the RDF file to convert and the format to convert to")
        exit()

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-i',
        '--inplace',
        help='if set, the file is converted in place, i.e. output file = input file',
        action='store_false'
        )

    parser.add_argument(
        "data",
        type=Path,
        help="Path to the RDF file or directory of files"
        )

    parser.add_argument(
        "ext",
        type=str,
        help="The RDFlib token for the RDF format you want to convert the RDF file to. Must be one of '{}'".format(
            ", ".join(RDF_FILE_ENDINGS.keys())
            ),
        choices=RDF_FILE_ENDINGS.keys()
        )

    parser.add_argument(
        "-p", "--prefixes",
        type=str,
        help="A path to a file containing a JSON dictionary of prefixes and their corresponding namespaces that will "
             "be bound to the graph. These will override any existing, conflicting, prefixes",
        default=None
        )

    args = parser.parse_args()

    prefixes = None
    if args.prefixes is not None:
        try:
            with open(args.prefixes, "r") as f:
                prefixes = json.load(f)

        except Exception as e:
            raise Exception(e)

    process_files(args)
