# rdfx
A small Python utility to convert, merge, and read/persist RDF data in different formats, across different "persistence systems".

## Use
The same command line arguments are used when running this script as either Python or BASH.
The command line utility at this point simplifies certain aspects of the merge/convert process.

### Python
Run the `rdfx.py` script with Python having installed the packages required by _requirements.txt_.

### BASH (Linux, Mac etc)
To utilise the command line util run:
`python rdfx.py *args`

For example to convert files:
`python rdfx.py convert -f nt -o output_dir myfile.ttl`
For Linux/Mac command line use, use `rdfx.sh`. This just calls the Python script, `rdfx.py` with your default Python installation and send it all the command line arguments. You must have the Python dependencies installed (see above) in your default Python environment to run this.

### Commands
These usage notes come from running the help command in the tool, e.g. `python rdfx.ph -h`:

```bash
usage: rdfx.py [-h] [--format {ttl,turtle,json,json-ld,jsonld,owl,xml,rdf,nt,n3}] [-o OUTPUT] [--comments COMMENTS] {convert,merge} data [data ...]

positional arguments:
  {convert,merge}
  data                  Path to the RDF file or directory of files for merging or conversion.

optional arguments:
  -h, --help            show this help message and exit
  --format {ttl,turtle,json,json-ld,jsonld,owl,xml,rdf,nt,n3}, -f {ttl,turtle,json,json-ld,jsonld,owl,xml,rdf,nt,n3}
                        The RDFlib token for the RDF format you want to convert the RDF file to.
  -o OUTPUT, --output OUTPUT
                        if set, the output location for merged or converted files, defaults to the current working directory
  --comments COMMENTS   Comments to prepend to the RDF, turtle only.
```


## License
LGPL - see the [LICENSE file](LICENSE) for details

## Dependencies
This uses [RDFlib](https://pypi.org/project/rdflib/).

## Contact
Original library:
**Nicholas J. Car**
*Data Systems Architect*
[SURROUND Australia Pty Ltd](http://surroundaustralia.com)
<nicholas.car@surroundaustralia.com>
GitHub: [nicholascar](https://github.com/nicholascar)
ORCID: <https://orcid.org/0000-0002-8742-7730>

Updates around persistence systems:
**David Habgood**
*Application Architect*
[SURROUND Australia Pty Ltd](https://surroundaustralia.com)
<david.habgood@surroundaustrlaia.com>
GitHub: [nicholascar](https://github.com/recalcitrantsupplant)
https://orcid.org/0000-0002-3322-1868
