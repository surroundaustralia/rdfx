import getpass
import io
import json
import warnings
from abc import ABC, abstractmethod
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from typing import List, Literal, Optional, Union
from urllib.parse import parse_qs

import httpx
from rdflib import Graph, URIRef

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
            sample_graph (Graph): The RDFlib Graph to persist. Only context-less graphs allowed.
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


class SOPGraph:
    def __init__(
        self,
        graph_type: str,
        uri: Optional[URIRef],
        name: Optional[str],
        parent: URIRef = None,
    ):
        self.graph_type = graph_type
        self.uri = uri
        self.name = name
        self.parent = parent

    # TODO complete


class SOP(PersistenceSystem):
    """
    Persist to an instance of SURROUND Ontology Platform (SOP)

    Args:
        system_iri (str): The IRI of the SOP system. Defaults to http://localhost:8083 (no trailing slash)
        repo_id (str): The ID of the repository on this GraphDB system to persist to
        graph_iri (str): The IRI of the graph to write to. Optional. Default is non (default graph)
        username (str): The username of a user on this SOP instance. Optional.
        password (str): The password of the user on this SOP instance. Optional.
        local (bool): Whether the SOP persistence system is for a local or remote SOP system
    """

    def __init__(
        self,
        system_iri: str = "http://localhost:8083",
        username: Optional[str] = "Administrator",
        password: Optional[str] = None,
    ):
        if not system_iri.startswith("http"):
            raise ValueError(
                f'The value you supplied for system_iri ({system_iri}) must start with "http" or "https"'
            )

        self.system_iri = system_iri
        self.username = username
        self.password = password
        self.client = None
        self.local = True if system_iri.startswith("http://localhost") else False

    def persist(self, g: Graph, graph_iri, leading_comments=None):
        if not (graph_iri.startswith("http") or graph_iri.startswith("urn")):
            raise ValueError(
                f"The value you supplied for graph_iri ({graph_iri}) is not valid"
            )
        if not self.client:
            self._create_client()

        content = g.serialize(format="turtle", encoding="utf-8")
        headers = {}
        if self.local:
            headers["Cookie"] = "username=Administrator"
        if graph_iri.startswith("urn:x-evn-tag"):
            projectGraph = SOP.graph_from_workflow(graph_iri)
        else:
            projectGraph = graph_iri
        form_data = {
            "_viewClass": "http://topbraid.org/teamwork#ImportRDFFileService",
            "projectGraph": projectGraph,
            "_base": graph_iri,
            "format": "turtle",
        }
        if graph_iri.startswith("urn:x-evn-tag"):
            form_data["tag"] = SOP.tag_from_workflow(graph_iri)
        response = self.client.post(
            self.system_iri + "/importFileUpload",
            data=form_data,
            files={"file": io.BytesIO(content)},
            headers=headers,
        )
        return parse_qs(response.text)["message"][0]

    def query(
        self,
        query,
        graph_iri,
        return_format: Optional[str] = "application/sparql-results+json",
    ):
        if not self.client:
            self._create_client()

        response = self.client.post(
            self.system_iri + "/sparql",
            data={
                "query": query,
                "with-imports": "false",
                "default-graph-uri": graph_iri,
            },
            headers={"Accept": return_format},
        )
        return response

    def asset_collection_size(self, asset_iri):
        """
        A wrapper around query to return the size of a given graph
        :param asset_iri:
        :return:
        """
        query = f"""SELECT (COUNT(*) as ?count) WHERE {{GRAPH <{asset_iri}> {{?s ?p ?o}} }}"""
        query_response = self.query(query, asset_iri)
        response_dict = json.loads(query_response.text)
        return int(response_dict["results"]["bindings"][0]["count"]["value"])

    def create_datagraph(
        self,
        datagraph_name: Optional[str] = None,
        description: Optional[str] = None,
        subjectArea: Optional[str] = None,
        default_namespace: Optional[str] = None,
        headers: Optional[dict] = None,
    ):

        if datagraph_name and datagraph_name.startswith("urn:x-evn-master"):
            datagraph_name = datagraph_name.strip("urn:x-evn-master:")
        if not datagraph_name:
            datagraph_name = f"Python_created_Datagraph_by_{getpass.getuser()}_at_{datetime.now().isoformat()}"
        if not default_namespace:
            default_namespace = f"https://data.surroundaustralia.com/data/{datagraph_name}#".replace(
                " ", "_"
            )
        if not subjectArea:
            subjectArea = ""
        if not description:
            description = ""
        # prepare the query
        if self.local:
            headers = {"Cookie": f"username=Administrator"}
        form_data = {
            "_viewClass": "http://topbraid.org/teamwork#CreateProjectService",
            "projectType": "http://teamwork.topbraidlive.org/datagraph/datagraphprojects#ProjectType",
            "subjectArea": subjectArea,
            "name": datagraph_name,
            "defaultNamespace": default_namespace,
            "comment": description,
        }

        response_dict = self._create_sop_asset(form_data, headers)
        datagraph_iri = f"urn:x-evn-master:{response_dict['id']}"
        return datagraph_iri

    def create_workflow(
        self,
        graph_iri: str,
        workflow_name: Optional[str] = None,
        headers: Optional[dict] = None,
    ):
        """
        :param headers: headers to add to the request
        :param graph_iri: The graph to add a workflow to
        :param workflow_name: The name of the workflow. If not provided, the current time is used
        :return: graph name
        """

        if not workflow_name:
            workflow_name = f"Python_created_Workflow_by_{getpass.getuser()}_at_{datetime.now().isoformat()}"

        form_data = {
            "_viewClass": "http://topbraid.org/teamwork#AddTagService",
            "projectGraph": graph_iri,
            "workflow": "http://topbraid.org/teamwork#DefaultTagWorkflowTemplate",
            "name": workflow_name,
            "comment": "",
        }

        response_dict = self._create_sop_asset(form_data, headers)

        # use the name SOP returns for the workflow
        workflow_name = response_dict["rootResource"].split(":")[2]
        workflow_graph_iri = f"{graph_iri}:{workflow_name}:{self.username}".replace(
            "urn:x-evn-master", "urn:x-evn-tag"
        )
        return workflow_graph_iri

    def create_vocabulary(
        self, vocabulary_name: Optional[str] = None, headers: Optional[dict] = None
    ):
        """
        :return:
        """
        pass

    def create_manifest(
        self,
        manifest_name: Optional[str] = None,
        description: Optional[str] = None,
        subjectArea: Optional[str] = None,
        default_namespace: Optional[str] = None,
        headers: Optional[dict] = None,
    ):
        """
        :param headers: headers to add to the request
        :param graph_iri: The graph to add a workflow to
        :param manifest_name: The name of the manifest. If not provided, the current time is used
        :return: graph name
        """
        # set defaults
        if not manifest_name:
            manifest_name = f"Python_created_Manifest_by_{getpass.getuser()}_at_{datetime.now().isoformat()}"
        if not default_namespace:
            default_namespace = f"https://data.surroundaustralia.com/manifest/{manifest_name}#".replace(
                " ", "_"
            )
        if not subjectArea:
            subjectArea = ""
        if not description:
            description = ""

        form_data = {
            "_viewClass": "http://topbraid.org/teamwork#CreateProjectService",
            "projectType": "http://surroundaustralia.com/ns/platform/OntologyRegister",
            "owlImports": [
                "urn:x-evn-master:sop_ontology_register_model",
                "https://data.surroundaustralia.com/def/standards-baseline",
            ],
            "name": manifest_name,
            "defaultNamespace": default_namespace,
            "subjectArea": subjectArea,
            "comment": description,
        }

        response_dict = self._create_sop_asset(form_data, headers)
        # use the name SOP returns for the workflow
        manifest_iri = f"urn:x-evn-master:{response_dict['id']}"
        return manifest_iri

    def asset_exists(self, asset_urn: str) -> bool:
        """
        Checks whether an asset exists in SOP, returns True or False
        :param asset_urn: The EDG URN of the asset
        :return: boolean
        """
        if not self.client:
            self._create_client()

        if asset_urn.startswith("urn:x-evn-tag"):
            if not self.asset_exists(self.graph_from_workflow(asset_urn)):
                return False
        query = f"ASK WHERE {{GRAPH <{asset_urn}> {{?s ?p ?o}} }}"
        response = self.client.post(
            self.system_iri + "/sparql",
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
        )
        return json.loads(response.text)["boolean"]

    def _create_sop_asset(self, form_data, headers: Optional[dict]):
        # set defaults
        if not headers:
            headers = {}
        if self.local:
            headers["Cookie"] = "username=Administrator"
        if not self.client:
            self._create_client()

        # send to SOP
        response = self.client.post(
            self.system_iri + "/swp",
            data=form_data,
            headers=headers,
            cookies=self.client.cookies,
        )
        response_dict = json.loads(response.text)
        keys = response_dict.keys()
        if "response" in keys:  # datagraph creation success
            if response_dict["response"].startswith("Successfully"):
                return response_dict
        elif "changed" in keys:  # workflkow creation success
            if response_dict["changed"]:
                return response_dict
        elif "error" in keys:
            if (
                response_dict["error"]
                == f"A working copy with the label {form_data['name']} already exists."
            ):
                print(
                    f"Asset {form_data['name']} already exists in SOP instance {self.system_iri}."
                )
                return response_dict
            else:
                raise ValueError(response_dict["error"])
        else:
            raise Exception(f"Failed to create {form_data['name']} graph on SOP")

    def _close(self):
        self.client.get(self.system_iri + "/purgeuser?app=edg")

    def _create_client(self):
        self.system_iri += "/tbl"
        self.client = httpx.Client()
        if self.system_iri.startswith("http://localhost"):
            pass  # auth is not required
        else:
            self.client.get(self.system_iri)
            auth_response = self.client.post(
                self.system_iri + "/j_security_check",
                data={
                    "j_username": self.username,
                    "j_password": self.password,
                    "login": "LOGIN",
                },
            )
            if auth_response.text:
                raise ValueError(
                    f"SOP authentication to {self.system_iri} unsuccessful. Check username and password. The "
                    f"HTML is too long to print in python"
                )

    @staticmethod
    def graph_from_workflow(workflow_graph):
        # example input workflow: "urn:x-evn-tag:datagraph_name:workflow_name:Administrator"
        if not workflow_graph.startswith("urn:x-evn-tag"):
            raise ValueError(
                "The workflow graph passed does not start with 'x-evn-tag' - it does not look like a SOP "
                "Workflow"
            )
        intermediate = workflow_graph.split(":")
        intermediate[1] = "x-evn-master"
        return ":".join(intermediate[:3])

    @staticmethod
    def tag_from_workflow(workflow_graph):
        # example input workflow: "urn:x-evn-tag:datagraph_name:workflow_name:Administrator"
        if not workflow_graph.startswith("urn:x-evn-tag"):
            raise ValueError(
                "The workflow graph passed does not start with 'x-evn-tag' - it does not look like a SOP "
                "Workflow"
            )
        workflow_name = workflow_graph.split(":")[3]
        return "urn:x-tags:" + workflow_name


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
