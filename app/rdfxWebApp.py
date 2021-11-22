import sys

sys.path.append(".")
sys.path.append("..")
import streamlit as st
from rdflib import Graph
from streamlitPS import (persistence_systems_radio,
                         persistence_systems_string_file)

from rdfx.persistence_systems import VALID_RDF_FORMATS, String

st.set_page_config(layout="wide")
g = None

st.title("RDFX")
read_col, write_col = st.columns(2)
read_col.subheader("Read")

# read
#####################################################
read_radio = read_col.radio("Read System", ("String", "File", "SOP", "Fuseki", "S3"))
read_ps = None
if read_radio in ["String", "File"]:
    comments, g = persistence_systems_string_file(read_radio, read_col, "read")
else:
    read_ps = persistence_systems_radio(read_radio, read_col, "read")

# read graph
#####################################################
if read_ps:
    read_graph = read_col.text_input(label="Read graph")
    if read_ps and read_graph:
        g = read_ps.read(read_graph)

# write
#####################################################
write_col.subheader("Write")
write_radio = write_col.radio("Write System", ("File", "SOP", "Fuseki", "S3"))
if write_radio == "File":
    write_ps = persistence_systems_string_file(write_radio, write_col, "write")
else:
    write_ps = persistence_systems_radio(write_radio, write_col, "write")

rdf_format = write_col.selectbox("Output format", options=VALID_RDF_FORMATS)
if rdf_format in ["turtle", "ttl"]:
    comments_radio = write_col.radio(
        "Output comments", ("From input", "Enter manually")
    )
    output_comments = write_col.text_input("Comments")

#####################################################
st.subheader("Preview")
if g:
    st.text(String().write(g, leading_comments=comments)[:1000])

    # valid_rdf = validate(rdf_string)
    # if valid_rdf:
    #     st.success("Valid input RDF")
    # else:
    #     st.error("Invalid input RDF")
# st.markdown(f"_Relative paths are allowed and are relative to: {os.getcwd()}_")
# st.write(
#     "*Note:* If you will be writing multiple graphs to different directories within a single repository, "
#     'specify the top level directory here, and file paths relative to this in the "Data Syncronisation" form'
#     " below."
# )
# with st.form(key="write_file_ps"):
#     write_directory = st.text_input(label="File write location", value="")
#     output_dir = Path(write_directory).resolve()
#     dir_check = st.form_submit_button(label="Set write directory")
# if dir_check:
#     if output_dir.exists():
#         st.success(f'Write directory "{str(output_dir)}" exists.')
#     else:
#         st.warning(
#             f'Write directory "{str(output_dir)}" does NOT exist - it WILL be created at runtime - '
#             "permissions permitting!"
#         )
#
# #############################
# st.subheader("Data Synchronisation")
# choice = st.radio("Select an option", ("Form", "JSON file"))
#
# if choice == "Form":
#     with st.form(key="sync_input_data"):
#         st.write("Add graphs to sync config")
#         read_item = st.text_input(
#             label="SOP Graph or Workflow", value="urn:x-evn-master:canonical_examples"
#         )
#         write_item = st.text_input(label="Optional: File to write to", value="")
#         if write_item:
#             kwargs = {"read_item": read_item, "write_item": write_item}
#         else:
#             kwargs = {"read_item": read_item}
#
#         read_col, write_col, col3, col4 = st.columns(4)
#         add_to_config = read_col.form_submit_button(label="Add to config")
#         reset_config = write_col.form_submit_button(label="Reset config")
#         dry_run = col3.form_submit_button(label="Dry Run")
#         sync = col4.form_submit_button(label="Sync Data")
#
#         st.write("Optional: save this configuration as JSON")
#         json_file = st.text_input(
#             label="path of JSON file to create", value="myconfig.json"
#         )
#         save_config_as_json = st.form_submit_button(label="Save config as JSON")
#
#     if add_to_config:
#         if "config" not in st.session_state:
#             st.session_state.config = {"graphs": [kwargs]}
#         else:
#             st.session_state.config["graphs"].append(kwargs)
#         st.sidebar.json(json.dumps(st.session_state.config))
#
#     # allow config to be reset
#     if reset_config:
#         st.session_state.clear()
#         st.success("Config reset")
#
#     if save_config_as_json:
#         if "config" not in st.session_state:
#             st.session_state.config = {"graphs": [kwargs]}
#         json_path = Path(json_file)
#         with json_path.open("w") as f:
#             json.dump(st.session_state.config, f, indent=4)
#         if json_path.exists():
#             st.success(f"Config saved in: {str(json_path.resolve())}")
#
#     if dry_run or sync:
#         if "config" not in st.session_state:
#             st.session_state.config = {"graphs": [kwargs]}
#         read_sop_ps = SOP(read_sop_iri, read_sop_username, read_sop_password)
#         output_file_path = Path(output_dir / write_item)
#         write_dir_ps = File(output_dir)
#         if dry_run:
#             # read check
#             if read_sop_ps.asset_exists(read_item):
#                 st.success(f"Read item: SOP Asset {read_item} exists on {read_sop_iri}")
#             else:
#                 st.error(
#                     f"Read item: SOP Asset {read_item} does NOT exist on {read_sop_iri}"
#                 )
#             # write check
#             if not write_item:
#                 st.warning(
#                     "Write item: SOP Asset name from read asset will be used as the filename"
#                 )
#             elif output_file_path.is_dir():
#                 st.error(f"Write item: Directory supplied. Only filenames are allowed.")
#             elif not output_file_path.exists():
#                 st.success(
#                     f"The file {str(output_file_path)} does not exist and will be created."
#                 )
#             elif output_file_path.exists():
#                 st.warning(
#                     f"The file {str(output_file_path)} exists and will be overwritten."
#                 )
#         elif sync:
#             prov_item = run_workflow(
#                 st.session_state.config, read_ps=read_sop_ps, write_ps=write_dir_ps
#             )
#             st.session_state.clear()
#             st.success(f"Successful sync: Prov written to {prov_item}")
#
# elif choice == "JSON file":
#     root = Path.cwd()
#     options = root.glob("**/*.json")
#     selected_file = st.selectbox(label="Select JSON config", options=options)
#     if selected_file:
#         with selected_file.open() as f:
#             json_config = json.load(f)
#         st.sidebar.json(json.dumps(json_config))
#
#         with st.form(key="sync_input_data"):
#             read_col, write_col = st.columns(2)
#             dry_run = read_col.form_submit_button(label="Dry Run")
#             sync = write_col.form_submit_button(label="Sync Data")
#         if dry_run:
#             # read check
#             st.write("not implemeted just yet, need to iterate through items")
#
#         elif sync:
#             read_sop_ps = SOP(read_sop_iri, read_sop_username, read_sop_password)
#             write_dir_ps = File(output_dir)
#
#             prov_item = run_workflow(
#                 json_config, read_ps=read_sop_ps, write_ps=write_dir_ps
#             )
#             st.session_state.clear()
#             st.success(f"Successful sync: Prov written to {prov_item}")
