import sys

sys.path.append(".")
sys.path.append("..")
import os
from io import BytesIO
from pathlib import Path

import streamlit as st
from dotenv import find_dotenv, load_dotenv
from rdflib import Graph

from rdfx.persistence_systems import PERSISTENCE_SYSTEMS, String


def persistence_systems_string_file(radio, container, read_write):
    comments = graph = None

    if radio == "String":
        rdf_string = container.text_area("RDF String", key=f"sf{radio}{container}")
        if rdf_string:
            comments, graph = String().read(rdf_string)
            # if container.button("Validate input RDF", key="read_string"):
        return comments, graph

    elif radio == "File":
        if read_write == "read":
            file_obj = container.file_uploader("Upload RDF file")
            if file_obj:
                file_string = file_obj.getvalue().decode()
                comments, graph = String().read(file_string)
            return comments, graph
        elif read_write == "write":
            write_file = Path(container.text_input(label="Output filename")).resolve()
            st.write(write_file)
            parent_dir = write_file.parent
            return PERSISTENCE_SYSTEMS[radio](parent_dir)


def persistence_systems_radio(radio, container, read_write):
    ps = None
    auth = container.radio(
        "Authentication",
        ("Unauthenticated", "Authenticated"),
        key=f"ps{radio}{container}",
    )
    if auth == "Authenticated":
        auth_options = container.radio(
            "Authentication Credentials",
            (
                "Automatically detect",
                "Select specific .env file",
                "Read from environment variables",
                "Manually enter",
            ),
            key=f"ac{radio}{container}",
        )
        if auth_options in [
            "Automatically detect",
            "Select specific .env file",
            "Read from environment variables",
        ]:
            if auth_options == "Automatically detect":
                if find_dotenv():
                    location = find_dotenv()
                    load_dotenv()
                    container.write(
                        f"Authentication credentials successfully loaded from .env file at:"
                    )
                    container.code(location)
                else:
                    container.error(
                        f"No .env file found in {os.getcwd()} or its parent directories. Please create one or use one of "
                        f"the other options."
                    )
            elif auth_options == "Select specific .env file":
                root = Path.cwd()
                options = root.glob("**/.env")
                selected_file = container.selectbox(
                    label="Select .env file",
                    options=options,
                    help="files ending with .env in the current working directory and its subdirectories are shown",
                )
                load_dotenv(selected_file)
            elif auth_options == "Read from environment variables":
                pass
            ps_location = os.getenv(
                f"{read_write.upper()}_{radio.upper()}_LOCATION", ""
            )
            if not ps_location:
                raise ValueError(
                    f'Environment Variable "{read_write.upper()}_{radio.upper()}_LOCATION" not set'
                )
            ps_username = os.getenv(
                f"{read_write.upper()}_{radio.upper()}_USERNAME", ""
            )
            ps_password = os.getenv(
                f"{read_write.upper()}_{radio.upper()}_PASSWORD", ""
            )
        elif auth_options == "Manually enter":
            container.write("Enter the relevant details below:")
            ps_location = container.text_input(label="Location")
            ps_username = container.text_input(label="Username")
            ps_password = container.text_input(label="Password")

        # auth checking
        with container.form(key="read_ps"):
            auth_submit = st.form_submit_button(label="Test authentication")
            if auth_submit:
                st.write("Authenticating...")
                ps = PERSISTENCE_SYSTEMS[radio](ps_location, ps_username, ps_password)
                if radio == "SOP":
                    test_connection = ps._create_client(test_connection=True)
                    if test_connection == True:
                        st.success("Authentication successful.")
                        mydict = {
                            f"{read_write.upper()}_{radio.upper()}_LOCATION": ps_location,
                            f"{read_write.upper()}_{radio.upper()}_USERNAME": ps_username,
                            f"{read_write.upper()}_{radio.upper()}_PASSWORD": "*****",
                        }
                        st.write(mydict)
                    else:
                        st.error(test_connection)
    return ps
