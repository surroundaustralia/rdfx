# Google-style docstrings: https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
from abc import ABC, abstractmethod

import requests
from rdflib import Graph
from typing import Literal, Optional, List
from pathlib import Path
import warnings

RDF_FORMATS = Literal[
    "turtle",
    "xml",
    "json-ld",
    "nt",
    "n3"
]


class PersistenceSystem(ABC):
    @abstractmethod
    def persist(self, g: Graph):
        """
        Persists the given Graph in the form implemented by this Persistence System

        Args:
            g (Graph): The RDFlib Graph to persist. Only context-less graphs allowed.
        """
        pass


class String(PersistenceSystem):
    """
    Persist as a string

    Args:
        rdf_format (str): The RDFlib RDF format to serialise the RDF to
        leading_comments (List[str]): Strings to add as comments to the start of the output.
                                      # will be automatically inserted at the start of each
    """

    def __init__(
            self,
            rdf_format: RDF_FORMATS,
            leading_comments: Optional[List[str]] = None
    ):
        if rdf_format not in RDF_FORMATS.__init__("__args__"):
            raise ValueError(
                f"The RDF format selected must be one of {', '.join(RDF_FORMATS.__init__('__args__'))}")

        if leading_comments is not None:
            if rdf_format != "turtle":
                raise ValueError(f"If leading_comments is provided, rdf_format must be turtle")
            if any(lc.startswith("#") for lc in leading_comments):
                raise ValueError(f"leading_comments may not start with #. It will be added")

        self.rdf_format = rdf_format
        self.leading_comments = leading_comments

    def persist(self, g: Graph):
        if self.leading_comments is None:
            return g.serialize(format=self.rdf_format)
        else:
            s = ""
            for lc in self.leading_comments:
                s += f"# {lc}\n"
            s += g.serialize(format=self.rdf_format)
            return s


class File(PersistenceSystem):
    """
    Persist as a file

    Args:
        file_path (Path): The path to the file to serialise to
        rdf_format (str): The RDFlib RDF format to serialise the RDF to
        leading_comments (List[str]): Strings to add as comments to the start of the output.
                                      # will be automatically inserted at the start of each
    """

    def __init__(
            self,
            file_path: Path,
            rdf_format: RDF_FORMATS,
            leading_comments: Optional[List[str]] = None
    ):
        if not isinstance(file_path, Path):
            raise ValueError(f"The file path must be of type Path")

        if rdf_format not in RDF_FORMATS.__init__("__args__"):
            raise ValueError(
                f"The RDF format selected must be one of {', '.join(RDF_FORMATS.__init__('__args__'))}")

        if leading_comments is not None:
            if rdf_format != "turtle":
                raise ValueError(f"If leading_comments is provided, rdf_format must be turtle")
            if any(lc.startswith("#") for lc in leading_comments):
                raise ValueError(f"leading_comments may not start with #. It will be added")

        self.file_path = file_path
        self.rdf_format = rdf_format
        self.leading_comments = leading_comments

    def persist(self, g: Graph):
        if self.leading_comments is None:
            g.serialize(destination=str(self.file_path), format=self.rdf_format)
            return True
        else:
            s = ""
            for lc in self.leading_comments:
                s += f"# {lc}\n"
            s += g.serialize(format=self.rdf_format)
            with open(self.file_path, "w") as f:
                f.write(s)
            return True


class S3(PersistenceSystem):
    pass


class GraphDB(PersistenceSystem):
    """
    Persist to an instance of GraphDB

    Args:
        system_iri (str): The IRI of the GraphDB system. Something like http://localhost:7200 (no training slash)
        repo_id (str): The ID of the repository on this GraphDB system to persist to
        graph_iri (str): The IRI of the graph to write to. Optional. Default is non (default graph)
        username (str): The username of a user on this GraphDB instance. Optional.
        password (str): The password of the user on this GraphDB instance. Optional.
    """

    def __init__(
            self,
            system_iri: str,
            repo_id: str,
            graph_iri: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
    ):

        if system_iri is None or not system_iri.startswith("http"):
            raise ValueError(f"The value you supplied for system_iri ({system_iri}) is not valid")

        if repo_id is None:
            raise ValueError(f"The value you supplied for repo_id cannot be None")

        if graph_iri is not None and not (graph_iri.startswith("http") or graph_iri.startswith("urn")):
            raise ValueError(f"The value you supplied for graph_iri ({graph_iri}) is not valid")

        self.system_iri = system_iri
        self.repo_id = repo_id
        self.graph_iri = graph_iri
        self.username = username
        self.password = password


class Fuseki(PersistenceSystem):
    """
    Persist to an instance of Fuseki

    Args:
        system_iri (str): The IRI of the GraphDB system. Something like http://localhost:7200 (no training slash)
        repo_id (str): The ID of the repository on this GraphDB system to persist to
        graph_iri (str): The IRI of the graph to write to. Optional. Default is non (default graph)
        username (str): The username of a user on this Fuseki instance. Optional.
        password (str): The password of the user on this Fuseki instance. Optional.
    """

    def __init__(
            self,
            system_iri: str,
            repo_id: str,
            graph_iri: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
    ):

        if system_iri is None or not system_iri.startswith("http"):
            raise ValueError(f"The value you supplied for system_iri ({system_iri}) is not valid")

        if repo_id is None:
            raise ValueError(f"The value you supplied for repo_id cannot be None")

        if graph_iri is not None and not (graph_iri.startswith("http") or graph_iri.startswith("urn")):
            raise ValueError(f"The value you supplied for graph_iri ({graph_iri}) is not valid")

        self.system_iri = system_iri
        self.repo_id = repo_id
        self.graph_iri = graph_iri
        self.username = username
        self.password = password


class SOP(PersistenceSystem):
    """
    Persist to an instance of SURROUND Ontology Platform (SOP)

    Args:
        system_iri (str): The IRI of the GraphDB system. Something like http://localhost:8083 (no training slash)
        repo_id (str): The ID of the repository on this GraphDB system to persist to
        graph_iri (str): The IRI of the graph to write to. Optional. Default is non (default graph)
        username (str): The username of a user on this SOP instance. Optional.
        password (str): The password of the user on this SOP instance. Optional.
    """

    def __init__(
            self,
            system_iri: str,
            graph_iri: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
    ):
        if system_iri is None or not system_iri.startswith("http"):
            raise ValueError(f"The value you supplied for system_iri ({system_iri}) is not valid")

        if graph_iri is not None and not (graph_iri.startswith("http") or graph_iri.startswith("urn")):
            raise ValueError(f"The value you supplied for graph_iri ({graph_iri}) is not valid")

        self.system_iri = system_iri
        self.graph_iri = graph_iri
        self.username = username
        self.password = password

    def persist(self, g: Graph):
        global saved_session_cookies

        if "localhost" not in self.system_iri:
            self.system_iri += "/edg"

        with requests.Session() as s:
            s.get(self.system_iri)
            reuse_sessions = False

            # should be able to check the response contains
            if reuse_sessions and saved_session_cookies:
                s.cookies = saved_session_cookies
            else:
                s.post(
                    self.system_iri + "/tbl/j_security_check",
                    data={"j_username": self.username, "j_password": self.password},
                )
                # detect success!
                if reuse_sessions:
                    saved_session_cookies = s.cookies

            # prepare the INSERT query
            q = ""
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")  # ignore the rdflib NT serializer warning
                q = "INSERT DATA {\n" + g.serialize(format="nt") + "\n}"

            response = s.post(
                self.system_iri + "/tbl/sparql",
                data={
                    "update": q,
                    "using-graph-uri": self.graph_iri
                },
                headers={"Accept": "application/sparql-results+json"},
            )

            # force logout of session
            s.get(self.system_iri + "/tbl/purgeuser?app=edg")

            return response
