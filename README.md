# rdfx
A small Python utility to convert RDF data formats, merge RDF data and persist it. This uses [RDFlib](https://pypi.org/project/rdflib/).

## Use
The same command line arguments are used when running this script as either Python or BASH.

### Python
Run the `rdfx.py` script with Python having installed the packages required by _requirements.txt_. 

### BASH (Linux, Mac etc)
For Linux/Mac command line use, use `rdfx.sh`. This just calls the Python script, `rdfx.py` with your default Python installation and send it all the command line arguments. You must have the Python dependencies installed (see above) in your default Python environment to run this.

### Commands
These usage notes come from running the help command in the tool, e.g. `sh rdfx.sh -h`:

```bash
usage: rdfx.py [-h] [-i] [-p PREFIXES]
               data {ttl,turtle,json,json-ld,jsonld,owl,xml,rdf,nt,n3}

positional arguments:
  data                  Path to the RDF file or directory of files
  {ttl,turtle,json,json-ld,jsonld,owl,xml,rdf,nt,n3}
                        The RDFlib token for the RDF format you want to
                        convert the RDF file to.

optional arguments:
  -h, --help            show this help message and exit
  -i, --inplace         if set, the file is converted in place, i.e. output
                        file = input file
  -p PREFIXES, --prefixes PREFIXES
                        A path to a file containing a JSON dictionary of
                        prefixes and their corresponding namespaces that will
                        be bound to the graph. These will override any
                        existing, conflicting, prefixes
```


## License
LGPL - see the [LICENSE file](LICENSE) for details


## Contact
**Nicholas J. Car**  
*Data Systems Architect*  
[SURROUND Australia Pty Ltd](http://surroundaustralia.com)  
<nicholas.car@surroundaustralia.com>  
GitHub: [nicholascar](https://github.com/nicholascar)  
ORCID: <https://orcid.org/0000-0002-8742-7730>
