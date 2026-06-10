# /// script
# dependencies = []
#
# [tool.griptape-nodes]
# name = "deadline_cloud_train_lora"
# description = "An example demonstrating how a NodeGroup can be used to send a LoRA training subflow up to AWS Deadline Cloud within a running workflow."
# schema_version = "0.14.0"
# engine_version_created_with = "0.66.2"
# node_libraries_referenced = [["AWS Deadline Cloud Library", "0.66.3"], ["Griptape Nodes Lora Training Library", "0.62.0"], ["Griptape Nodes Library", "0.55.0"]]
# node_types_used = [["Griptape Nodes Library", "EndFlow"], ["Griptape Nodes Library", "EngineNode"], ["Griptape Nodes Library", "MergeTexts"], ["Griptape Nodes Library", "Note"], ["Griptape Nodes Library", "PathJoin"], ["Griptape Nodes Library", "StandardSubflowNodeGroup"], ["Griptape Nodes Library", "StartFlow"], ["Griptape Nodes Lora Training Library", "DownloadDatasetNode"], ["Griptape Nodes Lora Training Library", "GenerateDatasetNode"], ["Griptape Nodes Lora Training Library", "TrainLoraNode"]]
# is_griptape_provided = true
# is_template = true
# creation_date = 2025-12-31T19:02:17.526052Z
# last_modified_date = 2025-12-31T19:02:17.772043Z
# workflow_shape = "{\"inputs\":{\"Start Flow\":{\"exec_out\":{\"name\":\"exec_out\",\"tooltip\":\"Connection to the next node in the execution chain\",\"type\":\"parametercontroltype\",\"input_types\":[\"parametercontroltype\"],\"output_type\":\"parametercontroltype\",\"default_value\":null,\"tooltip_as_input\":null,\"tooltip_as_property\":null,\"tooltip_as_output\":null,\"ui_options\":{\"display_name\":\"Flow Out\"},\"settable\":true,\"is_user_defined\":true,\"parent_container_name\":null,\"parent_element_name\":null}}},\"outputs\":{\"End Flow\":{\"exec_in\":{\"name\":\"exec_in\",\"tooltip\":\"Control path when the flow completed successfully\",\"type\":\"parametercontroltype\",\"input_types\":[\"parametercontroltype\"],\"output_type\":\"parametercontroltype\",\"default_value\":null,\"tooltip_as_input\":null,\"tooltip_as_property\":null,\"tooltip_as_output\":null,\"ui_options\":{\"display_name\":\"Succeeded\"},\"settable\":true,\"is_user_defined\":true,\"parent_container_name\":null,\"parent_element_name\":null},\"failed\":{\"name\":\"failed\",\"tooltip\":\"Control path when the flow failed\",\"type\":\"parametercontroltype\",\"input_types\":[\"parametercontroltype\"],\"output_type\":\"parametercontroltype\",\"default_value\":null,\"tooltip_as_input\":null,\"tooltip_as_property\":null,\"tooltip_as_output\":null,\"ui_options\":{\"display_name\":\"Failed\"},\"settable\":true,\"is_user_defined\":true,\"parent_container_name\":null,\"parent_element_name\":null},\"was_successful\":{\"name\":\"was_successful\",\"tooltip\":\"Indicates whether it completed without errors.\",\"type\":\"bool\",\"input_types\":[\"bool\"],\"output_type\":\"bool\",\"default_value\":false,\"tooltip_as_input\":null,\"tooltip_as_property\":null,\"tooltip_as_output\":null,\"ui_options\":{},\"settable\":false,\"is_user_defined\":true,\"parent_container_name\":null,\"parent_element_name\":null},\"result_details\":{\"name\":\"result_details\",\"tooltip\":\"Details about the operation result\",\"type\":\"str\",\"input_types\":[\"str\"],\"output_type\":\"str\",\"default_value\":null,\"tooltip_as_input\":null,\"tooltip_as_property\":null,\"tooltip_as_output\":null,\"ui_options\":{\"multiline\":true,\"placeholder_text\":\"Details about the completion or failure will be shown here.\"},\"settable\":false,\"is_user_defined\":true,\"parent_container_name\":null,\"parent_element_name\":null},\"output\":{\"name\":\"output\",\"tooltip\":\"New parameter\",\"type\":\"str\",\"input_types\":[\"any\"],\"output_type\":\"str\",\"default_value\":\"\",\"tooltip_as_input\":null,\"tooltip_as_property\":null,\"tooltip_as_output\":null,\"ui_options\":{\"multiline\":true,\"placeholder_text\":\"The merged text result.\",\"is_custom\":true,\"is_user_added\":true},\"settable\":true,\"is_user_defined\":true,\"parent_container_name\":\"\",\"parent_element_name\":null}}}}"
#
# ///

import argparse
import asyncio
import json
import pickle
from griptape_nodes.bootstrap.workflow_executors.local_workflow_executor import LocalWorkflowExecutor
from griptape_nodes.bootstrap.workflow_executors.workflow_executor import WorkflowExecutor
from griptape_nodes.drivers.storage.storage_backend import StorageBackend
from griptape_nodes.node_library.library_registry import IconVariant, NodeDeprecationMetadata, NodeMetadata
from griptape_nodes.retained_mode.events.connection_events import CreateConnectionRequest
from griptape_nodes.retained_mode.events.flow_events import (
    CreateFlowRequest,
    GetTopLevelFlowRequest,
    GetTopLevelFlowResultSuccess,
)
from griptape_nodes.retained_mode.events.library_events import LoadLibrariesRequest
from griptape_nodes.retained_mode.events.node_events import CreateNodeRequest
from griptape_nodes.retained_mode.events.parameter_events import (
    AddParameterToNodeRequest,
    AlterParameterDetailsRequest,
    SetParameterValueRequest,
)
from griptape_nodes.retained_mode.griptape_nodes import GriptapeNodes

GriptapeNodes.handle_request(LoadLibrariesRequest())

context_manager = GriptapeNodes.ContextManager()

if not context_manager.has_current_workflow():
    context_manager.push_workflow(workflow_name="deadline_cloud_train_lora")

"""
1. We've collated all of the unique parameter values into a dictionary so that we do not have to duplicate them.
   This minimizes the size of the code, especially for large objects like serialized image files.
2. We're using a prefix so that it's clear which Flow these values are associated with.
3. The values are serialized using pickle, which is a binary format. This makes them harder to read, but makes
   them consistently save and load. It allows us to serialize complex objects like custom classes, which otherwise
   would be difficult to serialize.
"""
top_level_unique_values_dict = {
    "7ef85a2b-ddd3-4a25-8a39-c27d7b863da7": pickle.loads(b"\x80\x04\x89."),
    "ed87d98d-b733-4643-b4ce-59de111463ac": pickle.loads(
        b"\x80\x04\x95\xde\x00\x00\x00\x00\x00\x00\x00\x8c\xdaYou can find your safetensor output file at: /tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_b39ejg7q/output/694d044bd3e04acdb467f5540857ffa5/dataSet_CHICKAPIGLET/my_flux2_klein_lora.safetensors\x94."
    ),
    "64b02f98-5bd0-4096-b882-a4af29890c3b": pickle.loads(
        b"\x80\x04\x95/\x00\x00\x00\x00\x00\x00\x00\x8c+You can find your safetensor output file at\x94."
    ),
    "4844d502-5f05-4f0f-9b96-cc63561a2a42": pickle.loads(
        b"\x80\x04\x95\x06\x00\x00\x00\x00\x00\x00\x00\x8c\x02: \x94."
    ),
    "eb508088-1760-4b93-b8f3-edcfe855d8b0": pickle.loads(b"\x80\x04\x95\x04\x00\x00\x00\x00\x00\x00\x00\x8c\x00\x94."),
    "4df9ef45-d3fe-4d6c-aa02-6bc276e88717": pickle.loads(
        b"\x80\x04\x95\x10\x00\x00\x00\x00\x00\x00\x00\x8c\x0c.safetensors\x94."
    ),
    "56e6e785-e534-4c7a-bab5-8f7cdad59ace": pickle.loads(
        b"\x80\x04\x95=\x00\x00\x00\x00\x00\x00\x00\x8c9You can find your safetensor output file at: .safetensors\x94."
    ),
    "2b65296f-28ef-4b0f-8b5e-0db454943569": pickle.loads(
        b"\x80\x04\x95\xe6\x03\x00\x00\x00\x00\x00\x00X\xdf\x03\x00\x00# Overview\n\nThis workflow runs a LoRA Training process on AWS Deadline Cloud and returns the generated safetensors file.\n\nClick the 'Run Workflow' button to kick off the training process!\n\n## Libraries\n\n* Griptape Nodes Standard Library: https://github.com/griptape-ai/griptape-nodes-library-standard\n* LoRA Training Library: https://github.com/griptape-ai/griptape-nodes-lora-training-library\n  * Note: Register the Library with the `griptape-nodes-library-cuda129.json` file for compatibility with Deadline\n* AWS Deadline Cloud Library: https://github.com/griptape-ai/griptape-nodes-library-deadline-cloud\n\n## NodeGroup\n\nAll of the Nodes in the below Subflow Node Group will be packaged and submitted to AWS Deadline Cloud as a Job. This will happen when the workflow runs. The output from the Job will include a safetensors file if the LoRA training was successful. The end of this workflow will display the file path location where the safetensors file was downloaded to on your machine.\x94."
    ),
    "44865051-a77b-43db-8a32-e0e73edb3ef3": pickle.loads(
        b"\x80\x04\x95\xab\x00\x00\x00\x00\x00\x00\x00]\x94(\x8c\x8d/tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_l00kpxc6/output/4a193e3b099f4b5ba070389f6fa2ac10/dataSet_CHICKAPIGLET\x94\x8c\x13my_flux2_klein_lora\x94e."
    ),
    "565f4882-aafc-4b6e-901c-bba007ebdf91": pickle.loads(
        b"\x80\x04\x95\x91\x00\x00\x00\x00\x00\x00\x00\x8c\x8d/tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_l00kpxc6/output/4a193e3b099f4b5ba070389f6fa2ac10/dataSet_CHICKAPIGLET\x94."
    ),
    "3cf9a784-62c0-431e-a32a-ca2b2ea92f27": pickle.loads(
        b"\x80\x04\x95\x17\x00\x00\x00\x00\x00\x00\x00\x8c\x13my_flux2_klein_lora\x94."
    ),
    "0455375a-ccf7-4990-a6d6-7c81fa17e363": pickle.loads(
        b"\x80\x04\x95\xa5\x00\x00\x00\x00\x00\x00\x00\x8c\xa1/tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_l00kpxc6/output/4a193e3b099f4b5ba070389f6fa2ac10/dataSet_CHICKAPIGLET/my_flux2_klein_lora\x94."
    ),
    "1baea03d-529b-4166-afba-8c452e6f7f3b": pickle.loads(
        b"\x80\x04\x95\x1e\x00\x00\x00\x00\x00\x00\x00\x8c\x1aAWS Deadline Cloud Library\x94."
    ),
    "ba2f5f50-7232-4e4e-a76d-702e375663e7": pickle.loads(
        b"\x80\x04\x95\x0e\x00\x00\x00\x00\x00\x00\x00\x8c\nTrain LoRA\x94."
    ),
    "0ea4d022-1d51-4be3-9453-a1017824b7d0": pickle.loads(
        b'\x80\x04\x95&\x00\x00\x00\x00\x00\x00\x00\x8c"Train a LoRA on AWS Deadline Cloud\x94.'
    ),
    "18294129-c8ff-44df-99f3-fbba7d2669c8": pickle.loads(b"\x80\x04]\x94."),
    "d62412cd-4db2-4656-b51c-4d4f3673b938": pickle.loads(b"\x80\x04]\x94."),
    "c76a001a-665e-4e24-b7f5-e7e3cde81c84": pickle.loads(b"\x80\x04K2."),
    "8db923e7-dbb7-465f-9223-89727049811d": pickle.loads(
        b"\x80\x04\x95\t\x00\x00\x00\x00\x00\x00\x00\x8c\x05READY\x94."
    ),
    "665b9876-ac69-4d15-8500-1be76a283644": pickle.loads(b"\x80\x04K\x00."),
    "43c55811-842d-4648-908f-e68b83a3e7ba": pickle.loads(
        b"\x80\x04\x95\x0f\x00\x00\x00\x00\x00\x00\x00\x8c\x0bconda-forge\x94."
    ),
    "b95f1dda-0795-4cdb-9837-eaf9a8f5cf3f": pickle.loads(
        b"\x80\x04\x95\x17\x00\x00\x00\x00\x00\x00\x00\x8c\x13python=3.12 pip git\x94."
    ),
    "df10d7d0-dd1a-44e1-bcb8-b89f674b39fb": pickle.loads(
        b"\x80\x04\x95\x15\x00\x00\x00\x00\x00\x00\x00}\x94(\x8c\x03min\x94K\x01\x8c\x03max\x94K\x02u."
    ),
    "0e54fd12-0243-46d1-8b38-ba5c9e48440d": pickle.loads(
        b"\x80\x04\x95K\x00\x00\x00\x00\x00\x00\x00\x8cGhttps://griptape-cloud-assets.s3.amazonaws.com/dataSet_CHICKAPIGLET.zip\x94."
    ),
    "cb6fcbf2-0df2-4041-9718-72380d060718": pickle.loads(
        b"\x80\x04\x95|\x00\x00\x00\x00\x00\x00\x00\x8cx/tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_l00kpxc6/output/4a193e3b099f4b5ba070389f6fa2ac10\x94."
    ),
    "1de7ea58-3cc4-4920-9fce-e1249cf3275c": pickle.loads(
        b"\x80\x04\x95\x91\x00\x00\x00\x00\x00\x00\x00\x8c\x8d/tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_l00kpxc6/output/4a193e3b099f4b5ba070389f6fa2ac10/dataSet_CHICKAPIGLET\x94."
    ),
    "c64e4ca1-8d68-4809-9ff4-545f74ab4634": pickle.loads(b"\x80\x04\x88."),
    "1ec0248e-0cab-4629-bf02-ad56313853b8": pickle.loads(
        b"\x80\x04\x95\x19\x00\x00\x00\x00\x00\x00\x00\x8c\x15GetConfigValueRequest\x94."
    ),
    "8a858144-67f5-4f3d-8b40-23ca41e38dd0": pickle.loads(
        b"\x80\x04\x95[\x00\x00\x00\x00\x00\x00\x00\x8cWSUCCESS: [10] Successfully returned the config value for section 'workspace_directory'.\x94."
    ),
    "0bd6826c-b377-43fd-8b87-48cdaf220e3e": pickle.loads(
        b"\x80\x04\x95\x17\x00\x00\x00\x00\x00\x00\x00\x8c\x13workspace_directory\x94."
    ),
    "e466f23b-8869-42ea-a30d-d5f5dc61b469": pickle.loads(b"\x80\x04K\x00."),
    "56fb461f-a0d7-4fd7-af45-71bf19e35597": pickle.loads(b"\x80\x04]\x94."),
    "c816aa75-86c3-49f9-91cc-0eeeb5ce8c08": pickle.loads(
        b"\x80\x04\x95s\x00\x00\x00\x00\x00\x00\x00\x8coDescribe this image with descriptive tags. Include details about the subject, setting, colors, mood, and style.\x94."
    ),
    "d722e225-57a1-441a-bad5-ca83e88db592": pickle.loads(b"\x80\x04]\x94."),
    "bf77fbb3-025c-4c51-970c-be3db5509ed2": pickle.loads(
        b"\x80\x04\x95\x0b\x00\x00\x00\x00\x00\x00\x00\x8c\x07cartoon\x94."
    ),
    "1d46f75b-0fa4-4d20-8592-711cf7d1a1f7": pickle.loads(b"\x80\x04\x95\x04\x00\x00\x00\x00\x00\x00\x00M\x00\x02."),
    "658fdcfd-2239-4479-8d56-50e9b4df08b8": pickle.loads(
        b"\x80\x04\x95\x9e\x00\x00\x00\x00\x00\x00\x00\x8c\x9a/tmp/NodeGroup_AWS_Deadline_Cloud_Library_packaged_flow_deadline_bundle_l00kpxc6/output/4a193e3b099f4b5ba070389f6fa2ac10/dataSet_CHICKAPIGLET/dataset.toml\x94."
    ),
    "98cafe19-90bb-475c-9c5b-2cfd8a91914f": pickle.loads(
        b"\x80\x04\x95\x0b\x00\x00\x00\x00\x00\x00\x00\x8c\x07Success\x94."
    ),
    "ea2eae76-755e-49c7-a8d3-27bf6cdff79f": pickle.loads(
        b"\x80\x04\x95\x1b\x00\x00\x00\x00\x00\x00\x00\x8c\x17Control Input Selection\x94."
    ),
    "442b70f4-0722-4e13-9726-89378c0d3856": pickle.loads(
        b"\x80\x04\x95\x10\x00\x00\x00\x00\x00\x00\x00\x8c\x0cFLUX.2 Klein\x94."
    ),
    "5ae39216-cf12-486d-9251-d26f629b9f2e": pickle.loads(
        b"\x80\x04\x95*\x00\x00\x00\x00\x00\x00\x00\x8c&black-forest-labs/FLUX.2-klein-base-4B\x94."
    ),
    "708c79f0-1b91-428d-ad4b-605adb1a2eb3": pickle.loads(
        b"\x80\x04\x95%\x00\x00\x00\x00\x00\x00\x00\x8c!black-forest-labs/FLUX.2-klein-4B\x94."
    ),
    "cdf4a83a-8809-419d-8870-a6d9ea0855ff": pickle.loads(
        b"\x80\x04\x95\x17\x00\x00\x00\x00\x00\x00\x00\x8c\x13my_flux2_klein_lora\x94."
    ),
    "851c044b-adeb-4f7a-9aae-9af57ae5060d": pickle.loads(
        b"\x80\x04\x95\n\x00\x00\x00\x00\x00\x00\x00G?\x1a6\xe2\xeb\x1cC-."
    ),
    "67745344-ebf8-411b-a3a9-0304cca3bdaa": pickle.loads(b"\x80\x04\x95\x04\x00\x00\x00\x00\x00\x00\x00M\xf4\x01."),
    "3af14b1f-483b-42e8-bae0-f2e2da803c9c": pickle.loads(b"\x80\x04\x95\x04\x00\x00\x00\x00\x00\x00\x00M\xdc\x05."),
    "f9ae4817-4699-4795-891c-98c7b00bf322": pickle.loads(b"\x80\x04K\x10."),
    "70ff63ff-ccda-414a-a4f1-2f88b59cf1bd": pickle.loads(
        b"\x80\x04\x95\x08\x00\x00\x00\x00\x00\x00\x00\x8c\x04bf16\x94."
    ),
    "5886642b-ec99-4f5f-ac6e-29ed5287166a": pickle.loads(b"\x80\x04K\n."),
    "2403920b-dd5d-43f3-a29b-949ce23b3eab": pickle.loads(b"\x80\x04\x89."),
    "92794cb3-1452-4477-b790-166ee0a93704": pickle.loads(b"\x80\x04K*."),
    "7b7eea6b-442c-4b15-9a93-29b23a3d0fd7": pickle.loads(
        b"\x80\x04\x951\x00\x00\x00\x00\x00\x00\x00\x8c-SUCCESS: LoRA training executed successfully.\x94."
    ),
}

"# Create the Flow, then do work within it as context."

flow0_name = GriptapeNodes.handle_request(
    CreateFlowRequest(parent_flow_name=None, flow_name="ControlFlow_1", set_as_new_context=False, metadata={})
).flow_name

with GriptapeNodes.ContextManager().flow(flow0_name):
    node0_name = GriptapeNodes.handle_request(
        CreateNodeRequest(
            node_type="StartFlow",
            specific_library_name="Griptape Nodes Library",
            node_name="Start Flow",
            metadata={
                "position": {"x": -1188.4680657484485, "y": -1758.09940346112},
                "tempId": "placing-1764036813381-cun22",
                "library_node_metadata": NodeMetadata(
                    category="workflows",
                    description="Define the start of a workflow and pass parameters into the flow",
                    display_name="Start Flow",
                    tags=None,
                    icon=None,
                    color=None,
                    group="create",
                    deprecation=None,
                    is_node_group=None,
                ),
                "library": "Griptape Nodes Library",
                "node_type": "StartFlow",
                "showaddparameter": True,
                "size": {"width": 940, "height": 1386},
                "category": "workflows",
            },
            resolution="resolved",
            initial_setup=True,
        )
    ).node_name
    node1_name = GriptapeNodes.handle_request(
        CreateNodeRequest(
            node_type="EndFlow",
            specific_library_name="Griptape Nodes Library",
            node_name="End Flow",
            metadata={
                "position": {"x": 7751.1778357033, "y": -1683.7637498379243},
                "tempId": "placing-1764036892906-ygdy5h",
                "library_node_metadata": NodeMetadata(
                    category="workflows",
                    description="Define the end of a workflow and return parameters from the flow",
                    display_name="End Flow",
                    tags=None,
                    icon=None,
                    color=None,
                    group="create",
                    deprecation=None,
                    is_node_group=None,
                ),
                "library": "Griptape Nodes Library",
                "node_type": "EndFlow",
                "showaddparameter": True,
                "size": {"width": 1298, "height": 1137},
                "category": "workflows",
            },
            initial_setup=True,
        )
    ).node_name
    with GriptapeNodes.ContextManager().node(node1_name):
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="output",
                default_value="",
                tooltip="New parameter",
                type="str",
                input_types=["any"],
                output_type="str",
                ui_options={
                    "multiline": True,
                    "placeholder_text": "The merged text result.",
                    "is_custom": True,
                    "is_user_added": True,
                },
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=True,
                parent_container_name="",
                initial_setup=True,
            )
        )
    node2_name = GriptapeNodes.handle_request(
        CreateNodeRequest(
            node_type="MergeTexts",
            specific_library_name="Griptape Nodes Library",
            node_name="Merge Texts",
            metadata={
                "position": {"x": 6474.82534181051, "y": -1683.7637498379243},
                "tempId": "placing-1764203125886-o0tu1c",
                "library_node_metadata": NodeMetadata(
                    category="text",
                    description="MergeTexts node",
                    display_name="Merge Texts",
                    tags=None,
                    icon="merge",
                    color=None,
                    group="merge",
                    deprecation=None,
                    is_node_group=None,
                ),
                "library": "Griptape Nodes Library",
                "node_type": "MergeTexts",
                "showaddparameter": False,
                "size": {"width": 1072, "height": 1143},
                "category": "text",
                "empty_merge_string_migrated": True,
            },
            initial_setup=True,
        )
    ).node_name
    node3_name = GriptapeNodes.handle_request(
        CreateNodeRequest(
            node_type="Note",
            specific_library_name="Griptape Nodes Library",
            node_name="Note",
            metadata={
                "position": {"x": 47.63740729474671, "y": -2564.9737855651842},
                "tempId": "placing-1764204840994-phevm",
                "library_node_metadata": NodeMetadata(
                    category="misc",
                    description="Create a note node to provide helpful context in your workflow",
                    display_name="Note",
                    tags=None,
                    icon="notepad-text",
                    color=None,
                    group="create",
                    deprecation=None,
                    is_node_group=None,
                ),
                "library": "Griptape Nodes Library",
                "node_type": "Note",
                "showaddparameter": False,
                "size": {"width": 1319, "height": 754},
                "category": "misc",
            },
            initial_setup=True,
        )
    ).node_name
    node4_name = GriptapeNodes.handle_request(
        CreateNodeRequest(
            node_type="PathJoin",
            specific_library_name="Griptape Nodes Library",
            node_name="Path Join",
            metadata={
                "position": {"x": 5276.137153270917, "y": -1683.7637498379243},
                "tempId": "placing-1764633161226-yyp6mk",
                "library_node_metadata": NodeMetadata(
                    category="files",
                    description="Join multiple path components together to create a file path using appropriate path separators",
                    display_name="Path Join",
                    tags=None,
                    icon="Combine",
                    color=None,
                    group=None,
                    deprecation=None,
                    is_node_group=None,
                ),
                "library": "Griptape Nodes Library",
                "node_type": "PathJoin",
                "showaddparameter": False,
                "size": {"width": 925, "height": 1146},
                "category": "files",
            },
            initial_setup=True,
        )
    ).node_name
    with GriptapeNodes.ContextManager().node(node4_name):
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="path_components_ParameterListUniqueParamID_03709160c98a4807a92babdc6f5b29bf",
                tooltip="Path components to join together.",
                type="str",
                input_types=["str"],
                output_type="str",
                ui_options={},
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=False,
                is_user_defined=True,
                settable=True,
                parent_container_name="path_components",
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="path_components_ParameterListUniqueParamID_3437e7970efe4a9995843409068d3b4c",
                tooltip="Path components to join together.",
                type="str",
                input_types=["str"],
                output_type="str",
                ui_options={},
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=False,
                is_user_defined=True,
                settable=True,
                parent_container_name="path_components",
                initial_setup=True,
            )
        )
    """# Create the Flow, then do work within it as context."""
    flow1_name = GriptapeNodes.handle_request(
        CreateFlowRequest(
            parent_flow_name=flow0_name,
            flow_name="Subflow Node Group_subflow",
            set_as_new_context=False,
            metadata={"flow_type": "NodeGroupFlow"},
        )
    ).flow_name
    with GriptapeNodes.ContextManager().flow(flow1_name):
        node5_name = GriptapeNodes.handle_request(
            CreateNodeRequest(
                node_type="DownloadDatasetNode",
                specific_library_name="Griptape Nodes Lora Training Library",
                node_name="Download Dataset",
                metadata={
                    "library_node_metadata": NodeMetadata(
                        category="lora",
                        description="Griptape Node that downloads and extracts a dataset zip from a URL.",
                        display_name="Download Dataset",
                        tags=None,
                        icon=None,
                        color=None,
                        group=None,
                        deprecation=None,
                        is_node_group=None,
                    ),
                    "library": "Griptape Nodes Lora Training Library",
                    "node_type": "DownloadDatasetNode",
                    "position": {"x": 1328.9081261812635, "y": 323.1440650926111},
                    "size": {"width": 902, "height": 884},
                    "showaddparameter": False,
                },
                initial_setup=True,
            )
        ).node_name
        node6_name = GriptapeNodes.handle_request(
            CreateNodeRequest(
                node_type="EngineNode",
                specific_library_name="Griptape Nodes Library",
                node_name="Engine Node",
                metadata={
                    "library_node_metadata": NodeMetadata(
                        category="engine",
                        description="Dynamically call any RequestPayload in the engine",
                        display_name="Engine Node",
                        tags=None,
                        icon="Cog",
                        color=None,
                        group=None,
                        deprecation=None,
                        is_node_group=None,
                    ),
                    "library": "Griptape Nodes Library",
                    "node_type": "EngineNode",
                    "position": {"x": 298, "y": 323.1440650926111},
                    "size": {"width": 801, "height": 897},
                    "showaddparameter": False,
                    "category": "engine",
                },
                initial_setup=True,
            )
        ).node_name
        with GriptapeNodes.ContextManager().node(node6_name):
            GriptapeNodes.handle_request(
                AddParameterToNodeRequest(
                    parameter_name="input_category_and_key",
                    tooltip="Input for category_and_key (type: str)",
                    type="str",
                    input_types=["str"],
                    output_type="str",
                    ui_options={"display_name": "category_and_key"},
                    mode_allowed_input=True,
                    mode_allowed_property=True,
                    mode_allowed_output=False,
                    is_user_defined=True,
                    initial_setup=True,
                )
            )
            GriptapeNodes.handle_request(
                AddParameterToNodeRequest(
                    parameter_name="input_failure_log_level",
                    tooltip="Input for failure_log_level (type: int)",
                    type="int",
                    input_types=["int"],
                    output_type="int",
                    ui_options={"display_name": "failure_log_level (optional)"},
                    mode_allowed_input=True,
                    mode_allowed_property=True,
                    mode_allowed_output=False,
                    is_user_defined=True,
                    initial_setup=True,
                )
            )
            GriptapeNodes.handle_request(
                AddParameterToNodeRequest(
                    parameter_name="output_success_value",
                    tooltip="Output from success result: value (type: all)",
                    type="all",
                    input_types=["all"],
                    output_type="all",
                    ui_options={"display_name": "value"},
                    mode_allowed_input=False,
                    mode_allowed_property=False,
                    mode_allowed_output=True,
                    is_user_defined=True,
                    initial_setup=True,
                )
            )
            GriptapeNodes.handle_request(
                AddParameterToNodeRequest(
                    parameter_name="output_failure_exception",
                    tooltip="Output from failure result: exception (type: exception)",
                    type="exception",
                    input_types=["exception"],
                    output_type="exception",
                    ui_options={"display_name": "exception (optional)"},
                    mode_allowed_input=False,
                    mode_allowed_property=False,
                    mode_allowed_output=True,
                    is_user_defined=True,
                    initial_setup=True,
                )
            )
        node7_name = GriptapeNodes.handle_request(
            CreateNodeRequest(
                node_type="GenerateDatasetNode",
                specific_library_name="Griptape Nodes Lora Training Library",
                node_name="Generate Lora Dataset",
                metadata={
                    "position": {"x": 2369.808914939722, "y": 323.1440650926111},
                    "tempId": "placing-1764036163069-5xh83",
                    "library_node_metadata": NodeMetadata(
                        category="lora",
                        description="Griptape Node that generates a dataset for LoRA training.",
                        display_name="Generate LoRA Dataset",
                        tags=None,
                        icon=None,
                        color=None,
                        group=None,
                        deprecation=None,
                        is_node_group=None,
                    ),
                    "library": "Griptape Nodes Lora Training Library",
                    "node_type": "GenerateDatasetNode",
                    "showaddparameter": False,
                    "size": {"width": 851, "height": 887},
                    "category": "lora",
                },
                initial_setup=True,
            )
        ).node_name
        with GriptapeNodes.ContextManager().node(node7_name):
            GriptapeNodes.handle_request(
                AlterParameterDetailsRequest(parameter_name="agent", ui_options={"hide": False}, initial_setup=True)
            )
            GriptapeNodes.handle_request(
                AlterParameterDetailsRequest(
                    parameter_name="agent_prompt", ui_options={"multiline": True, "hide": False}, initial_setup=True
                )
            )
            GriptapeNodes.handle_request(
                AlterParameterDetailsRequest(parameter_name="captions", ui_options={"hide": True}, initial_setup=True)
            )
        node8_name = GriptapeNodes.handle_request(
            CreateNodeRequest(
                node_type="TrainLoraNode",
                specific_library_name="Griptape Nodes Lora Training Library",
                node_name="Train Lora",
                metadata={
                    "position": {"x": 3395.905461692726, "y": 246},
                    "tempId": "placing-1764036252451-zq6id8",
                    "library_node_metadata": NodeMetadata(
                        category="lora",
                        description="Griptape Node that trains a LoRA model.",
                        display_name="Train LoRA",
                        tags=None,
                        icon=None,
                        color=None,
                        group=None,
                        deprecation=None,
                        is_node_group=None,
                    ),
                    "library": "Griptape Nodes Lora Training Library",
                    "node_type": "TrainLoraNode",
                    "showaddparameter": False,
                    "size": {"width": 1019, "height": 1072},
                    "category": "lora",
                },
                initial_setup=True,
            )
        ).node_name
        GriptapeNodes.handle_request(
            CreateConnectionRequest(
                source_node_name=node5_name,
                source_parameter_name="extracted_path",
                target_node_name=node7_name,
                target_parameter_name="dataset_folder",
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            CreateConnectionRequest(
                source_node_name=node7_name,
                source_parameter_name="dataset_config_path",
                target_node_name=node8_name,
                target_parameter_name="dataset_config_path",
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            CreateConnectionRequest(
                source_node_name=node6_name,
                source_parameter_name="output_success_value",
                target_node_name=node5_name,
                target_parameter_name="extract_location",
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            CreateConnectionRequest(
                source_node_name=node5_name,
                source_parameter_name="extracted_path",
                target_node_name=node8_name,
                target_parameter_name="output_dir",
                initial_setup=True,
            )
        )
        with GriptapeNodes.ContextManager().node(node5_name):
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="url",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["0e54fd12-0243-46d1-8b38-ba5c9e48440d"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="url",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["0e54fd12-0243-46d1-8b38-ba5c9e48440d"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="extract_location",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["cb6fcbf2-0df2-4041-9718-72380d060718"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="extract_location",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["cb6fcbf2-0df2-4041-9718-72380d060718"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="extracted_path",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["1de7ea58-3cc4-4920-9fce-e1249cf3275c"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="extracted_path",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["1de7ea58-3cc4-4920-9fce-e1249cf3275c"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["1de7ea58-3cc4-4920-9fce-e1249cf3275c"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node5_name,
                    value=top_level_unique_values_dict["1de7ea58-3cc4-4920-9fce-e1249cf3275c"],
                    initial_setup=True,
                    is_output=True,
                )
            )
        with GriptapeNodes.ContextManager().node(node6_name):
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="request_type",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["1ec0248e-0cab-4629-bf02-ad56313853b8"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="request_type",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["1ec0248e-0cab-4629-bf02-ad56313853b8"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["8a858144-67f5-4f3d-8b40-23ca41e38dd0"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["8a858144-67f5-4f3d-8b40-23ca41e38dd0"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="input_category_and_key",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["0bd6826c-b377-43fd-8b87-48cdaf220e3e"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="input_category_and_key",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["0bd6826c-b377-43fd-8b87-48cdaf220e3e"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="input_failure_log_level",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["e466f23b-8869-42ea-a30d-d5f5dc61b469"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="output_success_value",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["cb6fcbf2-0df2-4041-9718-72380d060718"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="output_success_value",
                    node_name=node6_name,
                    value=top_level_unique_values_dict["cb6fcbf2-0df2-4041-9718-72380d060718"],
                    initial_setup=True,
                    is_output=True,
                )
            )
        with GriptapeNodes.ContextManager().node(node7_name):
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="images",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["56fb461f-a0d7-4fd7-af45-71bf19e35597"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="generate_captions",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="generate_captions",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="agent_prompt",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c816aa75-86c3-49f9-91cc-0eeeb5ce8c08"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="agent_prompt",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c816aa75-86c3-49f9-91cc-0eeeb5ce8c08"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="captions",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["d722e225-57a1-441a-bad5-ca83e88db592"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="trigger_phrase",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["bf77fbb3-025c-4c51-970c-be3db5509ed2"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="trigger_phrase",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["bf77fbb3-025c-4c51-970c-be3db5509ed2"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="image_resolution",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["1d46f75b-0fa4-4d20-8592-711cf7d1a1f7"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="image_resolution",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["1d46f75b-0fa4-4d20-8592-711cf7d1a1f7"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="num_repeats",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="num_repeats",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="dataset_folder",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["1de7ea58-3cc4-4920-9fce-e1249cf3275c"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="dataset_folder",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["1de7ea58-3cc4-4920-9fce-e1249cf3275c"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="dataset_config_path",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["658fdcfd-2239-4479-8d56-50e9b4df08b8"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="dataset_config_path",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["658fdcfd-2239-4479-8d56-50e9b4df08b8"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["98cafe19-90bb-475c-9c5b-2cfd8a91914f"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node7_name,
                    value=top_level_unique_values_dict["98cafe19-90bb-475c-9c5b-2cfd8a91914f"],
                    initial_setup=True,
                    is_output=True,
                )
            )
        with GriptapeNodes.ContextManager().node(node8_name):
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="exec_out",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["ea2eae76-755e-49c7-a8d3-27bf6cdff79f"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="model_family",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["442b70f4-0722-4e13-9726-89378c0d3856"],
                    initial_setup=False,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="flux2_klein_model",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["5ae39216-cf12-486d-9251-d26f629b9f2e"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="output_name",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["cdf4a83a-8809-419d-8870-a6d9ea0855ff"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="learning_rate",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["851c044b-adeb-4f7a-9aae-9af57ae5060d"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="save_every_n_steps",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["67745344-ebf8-411b-a3a9-0304cca3bdaa"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="max_train_steps",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["3af14b1f-483b-42e8-bae0-f2e2da803c9c"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="network_dim",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["f9ae4817-4699-4795-891c-98c7b00bf322"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="network_alpha",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="mixed_precision",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["70ff63ff-ccda-414a-a4f1-2f88b59cf1bd"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="num_repeats",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["5886642b-ec99-4f5f-ac6e-29ed5287166a"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="randomize_seed",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["2403920b-dd5d-43f3-a29b-949ce23b3eab"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="seed",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["92794cb3-1452-4477-b790-166ee0a93704"],
                    initial_setup=False,
                    is_output=False,
                )
            )

            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="was_successful",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["c64e4ca1-8d68-4809-9ff4-545f74ab4634"],
                    initial_setup=True,
                    is_output=True,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["7b7eea6b-442c-4b15-9a93-29b23a3d0fd7"],
                    initial_setup=True,
                    is_output=False,
                )
            )
            GriptapeNodes.handle_request(
                SetParameterValueRequest(
                    parameter_name="result_details",
                    node_name=node8_name,
                    value=top_level_unique_values_dict["7b7eea6b-442c-4b15-9a93-29b23a3d0fd7"],
                    initial_setup=True,
                    is_output=True,
                )
            )
    node9_name = GriptapeNodes.handle_request(
        CreateNodeRequest(
            node_type="StandardSubflowNodeGroup",
            specific_library_name="Griptape Nodes Library",
            node_name="Subflow Node Group",
            metadata={
                "position": {"x": 47.63740729474671, "y": -1744.7637498379243},
                "size": {"width": 4747, "height": 1368},
                "library": "Griptape Nodes Library",
                "node_type": "StandardSubflowNodeGroup",
                "is_node_group": True,
                "execution_environment": {
                    "Griptape Nodes Library": {"start_flow_node": "StartFlow", "parameter_names": {}},
                    "AWS Deadline Cloud Library": {
                        "start_flow_node": "DeadlineCloudStartFlow",
                        "parameter_names": [
                            "deadlinecloudstartflow_job_name",
                            "deadlinecloudstartflow_job_description",
                            "deadlinecloudstartflow_attachment_input_paths",
                            "deadlinecloudstartflow_attachment_output_paths",
                            "deadlinecloudstartflow_priority",
                            "deadlinecloudstartflow_initial_state",
                            "deadlinecloudstartflow_max_failed_tasks",
                            "deadlinecloudstartflow_max_task_retries",
                            "deadlinecloudstartflow_farm_id",
                            "deadlinecloudstartflow_queue_id",
                            "deadlinecloudstartflow_storage_profile_id",
                            "deadlinecloudstartflow_conda_channels",
                            "deadlinecloudstartflow_conda_packages",
                            "deadlinecloudstartflow_run_on_all_worker_hosts",
                            "deadlinecloudstartflow_operating_system",
                            "deadlinecloudstartflow_cpu_architecture",
                            "deadlinecloudstartflow_vcpu",
                            "deadlinecloudstartflow_memory",
                            "deadlinecloudstartflow_gpus",
                            "deadlinecloudstartflow_gpu_memory",
                            "deadlinecloudstartflow_scratch_space",
                            "deadlinecloudstartflow_add_custom_amount_requirement",
                            "deadlinecloudstartflow_add_custom_attribute_requirement",
                        ],
                    },
                },
                "subflow_name": "Subflow Node Group_subflow",
                "expanded_dimensions": {"width": 4747, "height": 1368},
                "right_parameters": ["exec_out", "output_dir", "output_name"],
                "left_parameters": ["exec_in"],
                "library_node_metadata": NodeMetadata(
                    category="execution_flow",
                    description="Groups multiple nodes together for organization and parallel execution",
                    display_name="Subflow Node Group",
                    tags=None,
                    icon="Layers",
                    color=None,
                    group=None,
                    deprecation=None,
                    is_node_group=True,
                ),
                "executable": True,
            },
            node_names_to_add=[node5_name, node6_name, node7_name, node8_name],
        )
    ).node_name
    with GriptapeNodes.ContextManager().node(node9_name):
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="exec_out",
                tooltip="Enter control flow for exec_out.",
                type="parametercontroltype",
                input_types=["parametercontroltype"],
                output_type="parametercontroltype",
                ui_options={},
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=True,
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="output_dir",
                tooltip="Enter text/string for output_dir.",
                type="str",
                input_types=["str"],
                output_type="str",
                ui_options={},
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=True,
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="output_name",
                tooltip="Enter text/string for output_name.",
                type="str",
                input_types=["str"],
                output_type="str",
                ui_options={},
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=True,
                initial_setup=True,
            )
        )
        GriptapeNodes.handle_request(
            AddParameterToNodeRequest(
                parameter_name="exec_in",
                tooltip="Enter control flow for exec_in.",
                type="parametercontroltype",
                input_types=["parametercontroltype"],
                output_type="parametercontroltype",
                ui_options={},
                mode_allowed_input=True,
                mode_allowed_property=True,
                mode_allowed_output=True,
                initial_setup=True,
            )
        )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node2_name,
            source_parameter_name="output",
            target_node_name=node1_name,
            target_parameter_name="output",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node2_name,
            source_parameter_name="exec_out",
            target_node_name=node1_name,
            target_parameter_name="exec_in",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node4_name,
            source_parameter_name="exec_out",
            target_node_name=node2_name,
            target_parameter_name="exec_in",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node4_name,
            source_parameter_name="output",
            target_node_name=node2_name,
            target_parameter_name="input_3",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node8_name,
            source_parameter_name="exec_out",
            target_node_name=node9_name,
            target_parameter_name="exec_out",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node9_name,
            source_parameter_name="exec_out",
            target_node_name=node4_name,
            target_parameter_name="exec_in",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node8_name,
            source_parameter_name="output_dir",
            target_node_name=node9_name,
            target_parameter_name="output_dir",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node9_name,
            source_parameter_name="output_dir",
            target_node_name=node4_name,
            target_parameter_name="path_components_ParameterListUniqueParamID_03709160c98a4807a92babdc6f5b29bf",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node8_name,
            source_parameter_name="output_name",
            target_node_name=node9_name,
            target_parameter_name="output_name",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node9_name,
            source_parameter_name="output_name",
            target_node_name=node4_name,
            target_parameter_name="path_components_ParameterListUniqueParamID_3437e7970efe4a9995843409068d3b4c",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node0_name,
            source_parameter_name="exec_out",
            target_node_name=node9_name,
            target_parameter_name="exec_in",
            initial_setup=True,
        )
    )
    GriptapeNodes.handle_request(
        CreateConnectionRequest(
            source_node_name=node9_name,
            source_parameter_name="exec_in",
            target_node_name=node6_name,
            target_parameter_name="exec_in",
            initial_setup=True,
        )
    )
    with GriptapeNodes.ContextManager().node(node1_name):
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="was_successful",
                node_name=node1_name,
                value=top_level_unique_values_dict["7ef85a2b-ddd3-4a25-8a39-c27d7b863da7"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output",
                node_name=node1_name,
                value=top_level_unique_values_dict["ed87d98d-b733-4643-b4ce-59de111463ac"],
                initial_setup=True,
                is_output=False,
            )
        )
    with GriptapeNodes.ContextManager().node(node2_name):
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="input_1",
                node_name=node2_name,
                value=top_level_unique_values_dict["64b02f98-5bd0-4096-b882-a4af29890c3b"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="input_2",
                node_name=node2_name,
                value=top_level_unique_values_dict["4844d502-5f05-4f0f-9b96-cc63561a2a42"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="input_3",
                node_name=node2_name,
                value=top_level_unique_values_dict["eb508088-1760-4b93-b8f3-edcfe855d8b0"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="input_4",
                node_name=node2_name,
                value=top_level_unique_values_dict["4df9ef45-d3fe-4d6c-aa02-6bc276e88717"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="merge_string",
                node_name=node2_name,
                value=top_level_unique_values_dict["eb508088-1760-4b93-b8f3-edcfe855d8b0"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="whitespace",
                node_name=node2_name,
                value=top_level_unique_values_dict["7ef85a2b-ddd3-4a25-8a39-c27d7b863da7"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output",
                node_name=node2_name,
                value=top_level_unique_values_dict["56e6e785-e534-4c7a-bab5-8f7cdad59ace"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output",
                node_name=node2_name,
                value=top_level_unique_values_dict["56e6e785-e534-4c7a-bab5-8f7cdad59ace"],
                initial_setup=True,
                is_output=True,
            )
        )
    with GriptapeNodes.ContextManager().node(node3_name):
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="note",
                node_name=node3_name,
                value=top_level_unique_values_dict["2b65296f-28ef-4b0f-8b5e-0db454943569"],
                initial_setup=True,
                is_output=False,
            )
        )
    with GriptapeNodes.ContextManager().node(node4_name):
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="path_components",
                node_name=node4_name,
                value=top_level_unique_values_dict["44865051-a77b-43db-8a32-e0e73edb3ef3"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="path_components_ParameterListUniqueParamID_03709160c98a4807a92babdc6f5b29bf",
                node_name=node4_name,
                value=top_level_unique_values_dict["565f4882-aafc-4b6e-901c-bba007ebdf91"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="path_components_ParameterListUniqueParamID_3437e7970efe4a9995843409068d3b4c",
                node_name=node4_name,
                value=top_level_unique_values_dict["3cf9a784-62c0-431e-a32a-ca2b2ea92f27"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output",
                node_name=node4_name,
                value=top_level_unique_values_dict["0455375a-ccf7-4990-a6d6-7c81fa17e363"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output",
                node_name=node4_name,
                value=top_level_unique_values_dict["0455375a-ccf7-4990-a6d6-7c81fa17e363"],
                initial_setup=True,
                is_output=True,
            )
        )
    with GriptapeNodes.ContextManager().node(node9_name):
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="execution_environment",
                node_name=node9_name,
                value=top_level_unique_values_dict["1baea03d-529b-4166-afba-8c452e6f7f3b"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_job_name",
                node_name=node9_name,
                value=top_level_unique_values_dict["ba2f5f50-7232-4e4e-a76d-702e375663e7"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_job_description",
                node_name=node9_name,
                value=top_level_unique_values_dict["0ea4d022-1d51-4be3-9453-a1017824b7d0"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_attachment_input_paths",
                node_name=node9_name,
                value=top_level_unique_values_dict["18294129-c8ff-44df-99f3-fbba7d2669c8"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_attachment_output_paths",
                node_name=node9_name,
                value=top_level_unique_values_dict["d62412cd-4db2-4656-b51c-4d4f3673b938"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_priority",
                node_name=node9_name,
                value=top_level_unique_values_dict["c76a001a-665e-4e24-b7f5-e7e3cde81c84"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_initial_state",
                node_name=node9_name,
                value=top_level_unique_values_dict["8db923e7-dbb7-465f-9223-89727049811d"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_max_failed_tasks",
                node_name=node9_name,
                value=top_level_unique_values_dict["665b9876-ac69-4d15-8500-1be76a283644"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_max_task_retries",
                node_name=node9_name,
                value=top_level_unique_values_dict["665b9876-ac69-4d15-8500-1be76a283644"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_conda_channels",
                node_name=node9_name,
                value=top_level_unique_values_dict["43c55811-842d-4648-908f-e68b83a3e7ba"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_conda_packages",
                node_name=node9_name,
                value=top_level_unique_values_dict["b95f1dda-0795-4cdb-9837-eaf9a8f5cf3f"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_run_on_all_worker_hosts",
                node_name=node9_name,
                value=top_level_unique_values_dict["7ef85a2b-ddd3-4a25-8a39-c27d7b863da7"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="deadlinecloudstartflow_gpus",
                node_name=node9_name,
                value=top_level_unique_values_dict["df10d7d0-dd1a-44e1-bcb8-b89f674b39fb"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output_dir",
                node_name=node9_name,
                value=top_level_unique_values_dict["565f4882-aafc-4b6e-901c-bba007ebdf91"],
                initial_setup=True,
                is_output=False,
            )
        )
        GriptapeNodes.handle_request(
            SetParameterValueRequest(
                parameter_name="output_name",
                node_name=node9_name,
                value=top_level_unique_values_dict["3cf9a784-62c0-431e-a32a-ca2b2ea92f27"],
                initial_setup=True,
                is_output=False,
            )
        )


def _ensure_workflow_context():
    context_manager = GriptapeNodes.ContextManager()
    if not context_manager.has_current_flow():
        top_level_flow_request = GetTopLevelFlowRequest()
        top_level_flow_result = GriptapeNodes.handle_request(top_level_flow_request)
        if (
            isinstance(top_level_flow_result, GetTopLevelFlowResultSuccess)
            and top_level_flow_result.flow_name is not None
        ):
            flow_manager = GriptapeNodes.FlowManager()
            flow_obj = flow_manager.get_flow_by_name(top_level_flow_result.flow_name)
            context_manager.push_flow(flow_obj)


def execute_workflow(
    input: dict,
    storage_backend: str = "local",
    workflow_executor: WorkflowExecutor | None = None,
    pickle_control_flow_result: bool = False,
) -> dict | None:
    return asyncio.run(
        aexecute_workflow(
            input=input,
            storage_backend=storage_backend,
            workflow_executor=workflow_executor,
            pickle_control_flow_result=pickle_control_flow_result,
        )
    )


async def aexecute_workflow(
    input: dict,
    storage_backend: str = "local",
    workflow_executor: WorkflowExecutor | None = None,
    pickle_control_flow_result: bool = False,
) -> dict | None:
    _ensure_workflow_context()
    storage_backend_enum = StorageBackend(storage_backend)
    workflow_executor = workflow_executor or LocalWorkflowExecutor(storage_backend=storage_backend_enum)
    async with workflow_executor as executor:
        await executor.arun(flow_input=input, pickle_control_flow_result=pickle_control_flow_result)
    return executor.output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--storage-backend",
        choices=["local", "gtc"],
        default="local",
        help="Storage backend to use: 'local' for local filesystem or 'gtc' for Griptape Cloud",
    )
    parser.add_argument(
        "--json-input",
        default=None,
        help="JSON string containing parameter values. Takes precedence over individual parameter arguments if provided.",
    )
    parser.add_argument("--exec_out", default=None, help="Connection to the next node in the execution chain")
    args = parser.parse_args()
    flow_input = {}
    if args.json_input is not None:
        flow_input = json.loads(args.json_input)
    if args.json_input is None:
        if "Start Flow" not in flow_input:
            flow_input["Start Flow"] = {}
        if args.exec_out is not None:
            flow_input["Start Flow"]["exec_out"] = args.exec_out
    workflow_output = execute_workflow(input=flow_input, storage_backend=args.storage_backend)
    print(workflow_output)
