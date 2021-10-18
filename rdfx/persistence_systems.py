# Google-style docstrings: https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
import getpass
import json
import warnings
from abc import ABC, abstractmethod
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import List, Literal, Optional, Union

import requests
from rdflib import Graph

RDF_FORMATS = Literal["ttl", "turtle", "xml", "json-ld", "nt", "n3"]
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
    "n3": "n3",
}


class PersistenceSystem(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def persist(self, g: Graph, rdf_format, leading_comments, *args):
        """
        Persists the given Graph in the form implemented by this Persistence System

        Args:
            g (Graph): The RDFlib Graph to persist. Only context-less graphs allowed.
            :param g:
            :param leading_comments:
            :param rdf_format:
        """
        pass

    @staticmethod
    def leading_comment_validator(leading_comments, rdf_format):
        if leading_comments is not None:
            if rdf_format not in ("turtle", "ttl"):
                raise ValueError(
                    f"If leading_comments is provided, rdf_format must be turtle"
                )
            if any(lc.startswith("#") for lc in leading_comments):
                raise ValueError(
                    f"leading_comments may not start with #. It will be added"
                )

    @staticmethod
    def rdf_format_validator(rdf_format):
        if rdf_format not in RDF_FORMATS.__init__("__args__"):
            raise ValueError(
                f"The RDF format selected must be one of {', '.join(RDF_FORMATS.__init__('__args__'))}"
            )

    @staticmethod
    def generate_string(g, rdf_format, leading_comments):
        # validate the RDF format - all methods utilise the 'generate_string' static method so this will always be
        # called
        # PersistenceSystem.rdf_format_validator(rdf_format)
        if leading_comments is None:
            return g.serialize(format=rdf_format)
        else:
            PersistenceSystem.leading_comment_validator(leading_comments, rdf_format)
            s = ""
            for lc in leading_comments:
                s += f"# {lc}\n"
            # add a new line after the leading comments
            s += "\n"
            s += g.serialize(format=rdf_format)
            return s


class String(PersistenceSystem):
    """
    Persist as a string

    Args:
        rdf_format (str): The RDFlib RDF format to serialise the RDF to
        leading_comments (List[str]): Strings to add as comments to the start of the output.
                                      # will be automatically inserted at the start of each
    """

    def __init__(self):
        super().__init__()

    def persist(
        self,
        g: Graph,
        rdf_format: RDF_FORMATS = "turtle",
        leading_comments: Optional = None,
    ):
        return self.generate_string(g, rdf_format, leading_comments)


class File(PersistenceSystem):
    """
    Persist as a file

    Args:
        directory (Path): The path to the file to serialise to
        rdf_format (str): The RDFlib RDF format to serialise the RDF to, defaults to turtle
        leading_comments (List[str]): Strings to add as comments to the start of the output.
                                      # will be automatically inserted at the start of each
    """

    def __init__(self, directory: Union[Path, str]):
        super().__init__()

        if not isinstance(directory, (Path, str)):
            raise ValueError(f"The file path must be a string or pathlib Path")
        self.directory = Path(directory).resolve()

        if not self.directory.is_dir():
            self.directory.mkdir()

    def persist(
        self,
        g: Graph,
        filename: str,
        rdf_format: RDF_FORMATS = "ttl",
        leading_comments: Optional = None,
    ):

        file_path = self.directory / f"{filename}.{rdf_format}"
        s = self.generate_string(g, rdf_format, leading_comments)

        with file_path.open("w") as f:
            f.write(s)
        return file_path


class S3(PersistenceSystem):
    """
    Persist the graph to S3

    Args:
        bucket (str): The S3 bucket to persist to
        key (str): The name of the object to store in S3
        aws_key: The key part of the credentials to authenticate with AWS for this bucket
        aws_secret: The secret part of the credentials to authenticate with AWS for this bucket
        rdf_format (str): The RDFlib RDF format to serialise the RDF to
        leading_comments (List[str]): Strings to add as comments to the start of the output.
                                      # will be automatically inserted at the start of each
    """

    def __init__(
        self, bucket: str, aws_key: str, aws_secret: str, region: str = "ap-southeast-2"
    ):
        for item in [bucket, aws_key, aws_secret, region]:
            if not isinstance(item, str):
                raise ValueError(
                    f"{item} is of type {type(item)}, but must be a string"
                )

        self.bucket = bucket
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region

    def persist(
        self,
        g: Graph,
        filename: str,
        rdf_format: RDF_FORMATS = "ttl",
        leading_comments: Optional = None,
    ):
        filename = f"{filename}.{rdf_format}"
        s = self.generate_string(g, rdf_format, leading_comments)
        bytes_obj = BytesIO(s.encode("utf-8"))
        try:
            import boto3
            import botocore
        except ImportError:
            raise
        args = ["s3"]
        kwargs = {
            "aws_access_key_id": self.aws_key,
            "aws_secret_access_key": self.aws_secret,
            "region_name": self.region,
        }
        client = boto3.client(*args, **kwargs)
        response = client.put_object(Body=bytes_obj, Bucket=self.bucket, Key=filename)
        if response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK:
            return filename
        else:
            response.raise_for_status()


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
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):

        if system_iri is None or not system_iri.startswith("http"):
            raise ValueError(
                f"The value you supplied for system_iri ({system_iri}) is not valid"
            )

        if repo_id is None:
            raise ValueError(f"The value you supplied for repo_id cannot be None")

        self.system_iri = system_iri
        self.repo_id = repo_id
        self.username = username
        self.password = password

    def persist(self, g: Graph, graph_iri):
        if graph_iri is not None and not (
            graph_iri.startswith("http") or graph_iri.startswith("urn")
        ):
            raise ValueError(
                f"The value you supplied for graph_iri ({graph_iri}) is not valid"
            )
        raise NotImplemented


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
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):

        if system_iri is None or not system_iri.startswith("http"):
            raise ValueError(
                f"The value you supplied for system_iri ({system_iri}) is not valid"
            )

        if repo_id is None:
            raise ValueError(f"The value you supplied for repo_id cannot be None")

        self.system_iri = system_iri
        self.repo_id = repo_id
        self.username = username
        self.password = password

    def persist(self, g: Graph, graph_iri):
        if graph_iri is not None and not (
            graph_iri.startswith("http") or graph_iri.startswith("urn")
        ):
            raise ValueError(
                f"The value you supplied for graph_iri ({graph_iri}) is not valid"
            )
        raise NotImplemented


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
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        if system_iri is None or not system_iri.startswith("http"):
            raise ValueError(
                f'The value you supplied for system_iri ({system_iri}) must start with "http" or "https"'
            )

        self.system_iri = system_iri
        self.username = username
        self.password = password
        self.session = None

    def persist(self, g: Graph, graph_iri):
        if not (graph_iri.startswith("http") or graph_iri.startswith("urn")):
            raise ValueError(
                f"The value you supplied for graph_iri ({graph_iri}) is not valid"
            )
        if not self.session:
            self._create_session()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # ignore the rdflib NT serializer warning
            content = "INSERT DATA {\n" + g.serialize(format="nt") + "\n}"

        response = self.session.post(
            self.system_iri + "/sparql",
            data={"update": content, "using-graph-uri": graph_iri},
        )
        return response

    def query(
        self,
        query,
        graph_iri,
        return_format: Optional[str] = "application/sparql-results+json",
    ):
        if not self.session:
            self._create_session()

        response = self.session.post(
            self.system_iri + "/sparql",
            data={
                "query": query,
                "with-imports": "true",
                "default-graph-uri": graph_iri,
            },
            headers={"Accept": return_format},
        )
        return response

    def create_datagraph(
        self,
        datagraph_name: str = "",
        subjectArea: str = "",
        description: str = "",
        default_namespace: str = None,
        owl_imports: str = "",
        spin_imports: str = "",
    ):
        """
        Creates a datagraph in SOP
        :param name:
        :param subjectArea:
        :param description:
        :param default_namespace:
        :param owl_imports:
        :param spin_imports:
        :return:
        """
        if not self.session:
            self._create_session()

        if not datagraph_name:
            datagraph_name = f"Python_Created_Datagraph_From_{getpass.getuser()}_at_{datetime.now().isoformat()}"

        # TODO use slugifier instead - though there's no guarantee it will slugify the same as SOP - does this matter?
        if not default_namespace:
            default_namespace = f"https://data.surroundaustralia.com/data/{datagraph_name}#".replace(
                " ", "_"
            )
        # prepare the query
        datagraph_creation_endpoint = self.system_iri + "/swp"
        headers = {"Cookie": "username=Administrator"}
        qsa = {
            "_viewClass": "http://topbraid.org/teamwork#CreateProjectService",
            "projectType": "http://teamwork.topbraidlive.org/datagraph/datagraphprojects#ProjectType",
            "subjectArea": subjectArea,
            "owlImports": owl_imports,
            "spinImports": spin_imports,
            "name": datagraph_name,
            "defaultNamespace": default_namespace,
            "comment": description,
        }
        # send the datagraph creation payload
        response = self.session.get(
            datagraph_creation_endpoint,
            params=qsa,
            headers=headers,
            cookies=self.session.cookies,
        )
        response_dict = json.loads(response.text)
        assert response_dict["response"].split()[0] == "Successfully"
        workflow_graph_iri = f"urn:x-evn-master:{response_dict['id']}"
        return workflow_graph_iri

    def create_workflow(self, graph_iri: str, workflow_name: Optional[str] = None):
        """
        :param graph_iri: The graph to add a workflow to
        :param workflow_name: The name of the workflow. If not provided, the current time is used
        :return: graph name
        """
        if not self.session:
            self._create_session()

        if not workflow_name:
            workflow_name = f"Python_Created_Workflow_From_{getpass.getuser()}_at_{datetime.now().isoformat()}"

        # prepare the query
        workflow_creation_endpoint = self.system_iri + "/swp"
        headers = {"Cookie": "username=Administrator"}
        qsa = {
            "_viewClass": "http://topbraid.org/teamwork#AddTagService",
            "projectGraph": graph_iri,
            "workflow": "http://topbraid.org/teamwork#DefaultTagWorkflowTemplate",
            "name": workflow_name,
            "comment": "",
        }
        # send to SOP
        response = self.session.get(
            workflow_creation_endpoint,
            params=qsa,
            headers=headers,
            cookies=self.session.cookies,
        )
        response_dict = json.loads(response.text)

        # verify the response, either created, or up
        if "changed" in response_dict.keys():
            assert response_dict["changed"]
        elif "error" in response_dict.keys():
            assert (
                response_dict["error"]
                == f"A working copy with the label {workflow_name} already exists."
            )
        else:
            # TODO figure out what would cause the graph to fail to create, and what response is given
            raise Exception("Failed to create workflow graph on SOP")
        workflow_graph_iri = f"{graph_iri}:{workflow_name}:Administrator".replace(
            "urn:x-evn-master", "urn:x-evn-tag"
        )
        return workflow_graph_iri

    def asset_exists(self, asset_urn: str) -> bool:
        """
        Checks whether an asset exists in SOP, returns True or False
        :param asset_urn: The EDG URN of the asset
        :return: boolean
        """
        if not self.session:
            self._create_session()

        query = f"ASK WHERE {{GRAPH <{asset_urn}> {{?s ?p ?o}} }}"
        response = self.session.post(
            self.system_iri + "/sparql",
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
        )
        return json.loads(response.text)["boolean"]

    def _close(self):
        self.session.get(self.system_iri + "/purgeuser?app=edg")

    def _create_session(self):

        self.system_iri += "/tbl"
        with requests.Session() as s:
            s.get(self.system_iri)
            auth_response = s.post(
                self.system_iri + "/j_security_check",
                data={"j_username": self.username, "j_password": self.password},
            )
            self.session = s


def prepare_files_list(files: Union[str, list, Path]) -> list:
    if isinstance(files, (str, Path)):
        files = [files]
    elif isinstance(files, (list)):
        pass
    else:
        raise ValueError("You must pass a string, pathlib Path, or list of these")
    files_list = (
        []
    )  # [Path(file) if Path(file).is_file() else file.glob('*') for file in args.data ]
    for file in files:
        fp = Path(file)
        if fp.is_dir():
            for file_type in RDF_FILE_ENDINGS.keys():
                files_list.extend([file for file in fp.glob("*" + file_type)])
        elif fp.is_file():
            files_list.append(fp)
    return files_list
