import sys
from os import path
import glob
from rdflib import Graph, util
import argparse
from pathlib import Path
import json
from .persistence_systems import *


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
	"turtle": "ttl",
	"xml": "xml",
	"json-ld": "json-ld",
	"nt": "nt",
	"n3": "n3"
}


def get_input_format(file_path):
	input_format = util.guess_format(file_path)
	if input_format is None:
		if sys.argv[1].endswith("json-ld") or file_path.endswith("jsonld") or file_path.endswith("jsonld"):
			input_format = "json-ld"
		else:
			raise Exception("ERROR: Cannot guess the RDF format of input file {}".format(file_path))

	return input_format


def make_output_file_path(input_file_path, input_format, output_format, in_place):
	output_file_name = input_file_path.name.split(".")[:-1][0]

	if input_format == output_format and not in_place:
		output_file_name += ".new"

	output_file_name = output_file_name + "." + OUTPUT_FILE_ENDINGS.get(output_format)

	output_path = input_file_path.parent / output_file_name
	print("output file: {}".format(output_path))
	return output_path


def convert(input_file_path, input_format, output_file_path, output_format, prefixes=None):
	g = Graph().parse(str(input_file_path), format=input_format)
	if prefixes is not None:
		for k, v in prefixes.items():
			g.bind(k, v)
	g.serialize(destination=str(output_file_path), format=output_format)


def merge(rdf_files: List[Path]) -> Graph:
	"""
	Merges a given set of RDF files into one graph

	"""
	for f in rdf_files:
		if not f.name.endswith(tuple(RDF_FILE_ENDINGS.keys())):
			raise ValueError(
				f"Files to be merged must have a known RDF suffix (one of {', '.join(RDF_FILE_ENDINGS)})")

	g = Graph()
	for f in rdf_files:
		g.parse(f, format=RDF_FILE_ENDINGS[f.suffix.lstrip(".")])
	return g


def persist_to(persistence_system: PersistenceSystem, g: Graph):
	if not issubclass(type(persistence_system), PersistenceSystem):
		return ValueError(
			f"You must select of the the subclasses of PersistenceSystem to use for the persistence_system argument")
	else:
		persistence_system.persist(g)


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
		help="The RDFlib token for the RDF format you want to convert the RDF file to.",
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

	if Path.is_dir(args.data):
		print("converting directory {}".format(args.data))
		for f in Path(args.data).glob("*"):
			print("converting {}".format(f))

			input_format = get_input_format(f)
			output_file_path = make_output_file_path(f, input_format, RDF_FILE_ENDINGS.get(args.ext), args.inplace)

			convert(f, input_format, output_file_path, args.ext, prefixes)
	elif Path.is_file(args.data):
		print("converting {}".format(sys.argv[1]))

		output_file_path = make_output_file_path(
			args.data,
			get_input_format(args.data),
			RDF_FILE_ENDINGS.get(args.ext),
			args.inplace
		)

		convert(
			args.data,
			get_input_format(args.data),
			output_file_path,
			RDF_FILE_ENDINGS.get(args.ext),
			prefixes
		)
	else:
		raise ValueError(
			"The value you supplied for 'data' was {} but that is not a directory or file as required".format(args.data)
		)

	print("converted")
