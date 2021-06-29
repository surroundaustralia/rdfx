# rdfx
A small Python script to convert RDF data formats. This uses [RDFlib](https://pypi.org/project/rdflib/).

use: 

```
~$ rdfx.py [-d] FILE-DIR EXT [-i]
```

Command | Description
--- | ---  
`-d` | convert files in a directory  
`FILE-DIR` | an RDF file or directory of RDF files if -d used. The file or files must have file extensions of one of ttl, turtle, json, json-ld, jsonld, owl, xml, rdf, nt, n3  
`EXT` | the RDF format you want to convert to. Must be one of turtle, xml, json-ld, nt, n3  
`-i` | convert in place - overwrites existing file if to & from formats are the same  

For Linux/Mac command line use, use `convert.sh`


## License
LGPL - see the [LICENSE file](LICENSE) for details


## Contact
**Nicholas J. Car**  
*Data Systems Architect*  
[SURROUND Australia Pty Ltd](http://surroundaustralia.com)  
<nicholas.car@surroundaustralia.com>  
GitHub: [nicholascar](https://github.com/nicholascar)  
ORCID: <https://orcid.org/0000-0002-8742-7730>
