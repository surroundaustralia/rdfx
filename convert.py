import sys
from os import path
import glob
import rdflib


if "-h" not in sys.argv and len(sys.argv) < 3:
	print("ERROR: You must supply two command line arguments: the RDF file to convert and the format to convert to")
	exit()

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
	input_format = rdflib.util.guess_format(file_path)
	if input_format is None:
		if sys.argv[1].endswith("json-ld") or file_path.endswith("jsonld") or file_path.endswith("jsonld"):
			input_format = "json-ld"
		else:
			raise Exception("ERROR: Cannot guess the RDF format of input file {}".format(file_path))

	return input_format


def get_output_format(s):
	output_format = RDF_FILE_ENDINGS.get(s)
	if output_format is None:
		raise ValueError(
			"ERROR: The output format you specified, {}, is not recognised. Must be one of {}"
				.format(
					s,
					", ".join(RDF_FILE_ENDINGS.keys())
				)
			)
	return output_format


def make_output_file_path(input_file_path, input_format, output_format, in_place):
	output_file_path = input_file_path.split(".")[:-1][0]

	if input_format == output_format and not in_place:
		output_file_path += ".new"

	return output_file_path + "." + OUTPUT_FILE_ENDINGS.get(output_format)


def convert(input_file_path, input_format, output_file_path, output_format):
	g = rdflib.Graph().parse(input_file_path, format=input_format)
	g.serialize(output_file_path, format=output_format)


if __name__ == "__main__":
	if "-h" in sys.argv:
		print()
		print("rdfx - an RDF format conversion script")
		print("--------------------------------------")
		print()
		print("use: ~$ convert.py [-d] FILE-DIR EXT [-i]")
		print()
		print("-d		- convert files in a directory ")
		print("FILE-DIR	- an RDF file or directory of RDF files if -d used. The file or files must have file "
			  "extensions of one of {}".format(", ".join(RDF_FILE_ENDINGS.keys())))
		print("EXT		- the RDF format you want to convert to. Must be one of {}"
			  .format(", ".join(OUTPUT_FILE_ENDINGS)))
		print("-i		- convert in place - overwrites existing file if to & from formats are the same")

		print()
		exit()

	if "-i" in sys.argv:
		in_place = True
	else:
		in_place = False

	if sys.argv[1] == "-d":
		# a directory is selected so list all files in it and, if we can work put the RDF format, convert them
		output_format = get_output_format(sys.argv[3])

		print("converting directory {}".format(sys.argv[2]))
		for f in glob.glob(path.join(sys.argv[2], "*")):
			print("converting {}".format(f))

			input_format = get_input_format(f)
			output_file_path = make_output_file_path(f, input_format, output_format, in_place)

			convert(f, input_format, output_file_path, output_format)
	else:
		print("converting {}".format(sys.argv[1]))

		input_file_path = sys.argv[1]
		input_format = get_input_format(input_file_path)
		output_format = get_output_format(sys.argv[2])
		output_file_path = make_output_file_path(input_file_path, input_format, output_format, in_place)

		convert(input_file_path, input_format, output_file_path, output_format)

	print("converted")
