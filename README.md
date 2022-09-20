![](https://surroundaustralia.com/themes/custom/surround_australia/surround-logo-dark.svg)

# rdfx

A small Python utility to convert, merge, and read/persist RDF data in different formats, across different "persistence
systems".

## How to Use

The command line utility covers merge and conversion functionality, and simplifies certain aspects of this. The

### Python

Run the `rdfx.py` script with Python having installed the packages required by _requirements.txt_.

### BASH (Linux, Mac etc)

To utilise the command line util run:
`python rdfx.py *args`

To convert a file:
`python rdfx.py convert myfile.ttl -f nt -o output_dir`
For multiple files:
`python rdfx.py convert myfile1.ttl myfile2.ttl -f nt -o output_dir`
A directory of files:
`python rdfx.py convert files_dir -f nt -o output_dir`
To merge multiple files:
`python rdfx.py merge myfile1.ttl myfile2.ttl -f nt -o output_dir`
To merge a directory of files:
`python rdfx.py merge files_dir -f nt -o output_dir`
To remove sort and remove unused prefixes in a turtle file:
`python rdfx.py clean myfile.ttl`

To simplify usage of the command line utility at present, the following behaviour has been set:

Type | Output Filenames
---|---
Merge | merged.{format}
Convert | file1.{format} file2.{format} ...

That is, when merging, the output filename will be "merged", with the correct file format.
When converting, the output filename will be the same as the input filename, with the correct file format.
This behaviour simplifies input to the command line util, allowing multiple files and directories to be input without
confusion as to which specified filenames are for input or output, and mappings between input and output, especially
directories or multiple files are converted/merged.

The python utilities behind the command line tool can be configured to set user specified filenames, for these cases
use Python.

### SOP / EDG usage

The SOP persistence system can be used to read and write to/from EDG master graphs and workflows. The SOP persistence
system can be instantiated with the following optional parameters:

1. location, defaults to "http://localhost:8083"
2. username, defaults to "Administrator"
3. password, defaults to ""
4. timeout, defaults to 60 seconds
   Example instantiation with defaults:

```
from rdfx.persistence_systems import SOP
local_sop_ps = SOP()
```

The following methods are available on instances of the SOP class:

| Method                | Paramters                                                                                                                                            | Returns                           |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|
| read                  | graph URN<br/> rdf_format                                                                                                                            | list of comments<br/>RDFLib Graph |
| write                 | RDFLib Graph<br/> graph IRI<br/> list of comments (optional)                                                                                         | The IRI of the created graph      |
| query                 | query<br/> graph_iri<br/> return_format                                                                                                              | The query results                 |
| asset_collection_size | asset_iri                                                                                                                                            | Triples count for the given asset |
| create_datagraph      | datagraph_name (optional) <br/>description (optional)<br/> subjectArea (optional)<br/> default_namespace (optional)<br/>HTTP  headers (optional)     | datagraph IRI                     |
| create_workflow       | graph_iri<br/> workflow_name (optional)<br/>HTTP  headers (optional)                                                                                 | workflow IRI                      |
| create_manifest       | manifest_name (optional)<br/> description (optional)<br/> subjectArea (optional)<br/> default_namespace (optional)<br/> HTTP headers (optional)<br/> | the IRI for the manifest          |
| asset_exists          | graph_name                                                                                                                                           | true/false                        |

### Command line tool documentation

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
