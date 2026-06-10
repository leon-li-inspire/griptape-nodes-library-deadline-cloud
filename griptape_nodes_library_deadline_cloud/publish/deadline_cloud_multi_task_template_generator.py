"""Multi-task job template generation for AWS Deadline Cloud workflows.

This module generates Open Job Description templates that execute the same workflow
multiple times as separate tasks within a single Deadline Cloud job. Each task
processes a different input from the items list.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger("multi_task_template_generator")


class DeadlineCloudMultiTaskJobTemplateGenerator:
    """Handles generation of Open Job Description templates for multi-task Griptape workflows.

    Unlike the standard DeadlineCloudJobTemplateGenerator which creates a single-task job,
    this generator creates a job with a parameterSpace that spawns N tasks, one for each
    item in the input list.
    """

    @staticmethod
    def generate_job_template(  # noqa: PLR0913
        job_bundle_dir: Path,
        workflow_name: str,
        library_paths: list[str],
        task_count: int,
        *,
        pickle_control_flow_result: bool = False,
        host_requirements: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Generate Open Job Description template for a multi-task workflow.

        Args:
            job_bundle_dir: Directory where the job bundle files are stored
            workflow_name: Name of the workflow being executed
            library_paths: List of library paths needed for workflow execution
            task_count: Number of tasks to create (one per iteration item)
            pickle_control_flow_result: Whether to pickle control flow results
            host_requirements: Optional host requirements dict with 'amounts' and/or 'attributes'

        Returns:
            The generated job template dictionary
        """
        parameter_definitions: list[dict[str, Any]] = []

        parameter_definitions.append(
            {
                "name": "DataDir",
                "type": "PATH",
                "objectType": "DIRECTORY",
                "dataFlow": "INOUT",
                "description": "Directory containing job attachments and workflow files",
            }
        )

        parameter_definitions.append(
            {
                "name": "LocationToRemap",
                "type": "PATH",
                "description": "Top level directory to remap in the job attachments",
            }
        )

        parameter_definitions.append(
            {
                "name": "ModelsLocationToRemap",
                "type": "PATH",
                "description": "Directory path to remap HuggingFace model cache location",
            }
        )

        parameter_definitions.append(
            {
                "name": "TaskCount",
                "type": "INT",
                "default": task_count,
                "description": "Number of tasks to execute",
            }
        )

        parameter_definitions.append(
            {
                "name": "OutputDir",
                "type": "STRING",
                "description": "Output folder subdirectory within DataDir",
            }
        )

        parameter_definitions.append(
            {
                "name": "CondaChannels",
                "type": "STRING",
                "description": "Conda channels to install packages from",
                "default": "conda-forge",
            }
        )

        parameter_definitions.append(
            {
                "name": "CondaPackages",
                "type": "STRING",
                "description": "Conda packages install job",
                "default": "python=3.12 git",
            }
        )

        # Generate Python execution script
        python_script = DeadlineCloudMultiTaskJobTemplateGenerator._generate_python_execution_script(
            library_paths, pickle_control_flow_result=pickle_control_flow_result
        )

        venv_script = """#!/bin/env bash
set -e
echo 'Setting up Python virtual environment...'
python -m pip install --upgrade pip wheel setuptools
echo 'Installing dependencies...'
pip install -r {{Param.LocationToRemap}}/assets/requirements.txt
mkdir -p {{Param.LocationToRemap}}/output

# Create .venv symlinks in libraries so library code that expects its own
# venv (e.g. _get_library_env_python) finds a working Python with all deps.
SESSION_PYTHON=$(which python)
for lib_dir in {{Param.LocationToRemap}}/assets/libraries/*/; do
    if [ -f "${lib_dir}griptape-nodes-library.json" ] || [ -f "${lib_dir}griptape-nodes-library-cuda129.json" ]; then
        mkdir -p "${lib_dir}.venv/bin"
        ln -sf "$SESSION_PYTHON" "${lib_dir}.venv/bin/python"
        echo "Created .venv symlink in ${lib_dir}"
    fi
done

echo 'Virtual environment setup complete.'
"""

        # Create job template with parameterSpace for multi-task execution
        job_template: dict[str, Any] = {
            "specificationVersion": "jobtemplate-2023-09",
            "name": f"Griptape Multi-Task Workflow: {workflow_name}",
            "parameterDefinitions": parameter_definitions,
            "jobEnvironments": [
                {
                    "name": "Python312_Venv",
                    "description": "Python 3.12 virtual environment setup",
                    "script": {
                        "actions": {
                            "onEnter": {
                                "command": "bash",
                                "args": ["{{Env.File.Enter}}"],
                            }
                        },
                        "embeddedFiles": [{"name": "Enter", "type": "TEXT", "runnable": True, "data": venv_script}],
                    },
                },
            ],
            "steps": [
                {
                    "name": "GriptapeWorkflow",
                    "parameterSpace": {
                        "taskParameterDefinitions": [
                            {
                                "name": "TaskIndex",
                                "type": "INT",
                                "range": f"0-{task_count - 1}",
                            }
                        ]
                    },
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": "python",
                                "args": [
                                    "{{Task.File.Run}}",
                                    "--input-file",
                                    "{{Param.LocationToRemap}}/assets/inputs/input_{{Task.Param.TaskIndex}}.json",
                                    "--task-index",
                                    "{{Task.Param.TaskIndex}}",
                                ],
                            }
                        },
                        "embeddedFiles": [{"name": "Run", "type": "TEXT", "runnable": True, "data": python_script}],
                    },
                },
            ],
        }

        # Add host requirements to the step if provided
        if host_requirements and len(host_requirements) > 0:
            job_template["steps"][0]["hostRequirements"] = host_requirements

        template_path = job_bundle_dir / "template.yaml"
        with template_path.open("w", encoding="utf-8") as template_file:
            yaml.dump(job_template, template_file, default_flow_style=False, sort_keys=False)

        logger.info("Multi-task job template written to: %s (task_count=%d)", template_path, task_count)

        return job_template

    @staticmethod
    def _generate_python_execution_script(library_paths: list[str], *, pickle_control_flow_result: bool = False) -> str:
        """Generate the Python script that will execute the Griptape workflow for each task.

        This script is similar to the standard execution script but:
        1. Accepts a --task-index argument to identify which task is running
        2. Writes output to a task-specific output file (output_N.json)
        """
        library_paths_str = ", ".join(repr(path) for path in library_paths)

        return f"""import argparse
import json
import logging
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

LIBRARIES = [str(Path(path)) for path in [{library_paths_str}]]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set up paths - use DataDir parameter for job attachments
location_to_remap = r"{{{{Param.LocationToRemap}}}}"
models_location_to_remap = r"{{{{Param.ModelsLocationToRemap}}}}"
output_dir_subdirectory = r"{{{{Param.OutputDir}}}}"

job_assets_dir = Path(location_to_remap) / "assets"
output_dir = Path(location_to_remap) / "output" / output_dir_subdirectory
output_dir.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(job_assets_dir))

# Copy the contents of static_files/ into the tracked output directory so that
# project-relative directory macros (e.g. {{outputs}}) resolve inside it.
_bundled_static_files = job_assets_dir / "static_files"
if _bundled_static_files.exists():
    shutil.copytree(str(_bundled_static_files), str(output_dir), dirs_exist_ok=True)
    logger.info("Copied static files contents into output directory: %s", output_dir)

# Load environment variables
if (job_assets_dir / ".env").exists():
    load_dotenv(str(job_assets_dir / ".env"))

# Set HuggingFace hub cache directory for model cache, and print
_hf_cache = Path(models_location_to_remap)
if not _hf_cache.exists() or not os.access(str(_hf_cache), os.W_OK):
    _hf_cache = Path(location_to_remap) / "hf_cache"
    _hf_cache.mkdir(parents=True, exist_ok=True)
    logger.info("Models path not writable, using fallback: %s", _hf_cache)
os.environ["HF_HUB_CACHE"] = str(_hf_cache)
os.environ["GTN_CONFIG_WORKSPACE_DIRECTORY"] = str(output_dir)
logger.info(f"HuggingFace model cache directory set to: {{os.environ['HF_HUB_CACHE']}}")
logger.info(f"Griptape workspace directory set to: {{os.environ['GTN_CONFIG_WORKSPACE_DIRECTORY']}}")

def _load_project_template(project_path: Path) -> None:
    # Load and activate the project template before libraries are registered
    from griptape_nodes.retained_mode.events.project_events import (  # noqa: PLC0415
        LoadProjectTemplateRequest,
        LoadProjectTemplateResultSuccess,
        SetCurrentProjectRequest,
    )
    from griptape_nodes.retained_mode.griptape_nodes import GriptapeNodes  # noqa: PLC0415

    load_result = GriptapeNodes.handle_request(LoadProjectTemplateRequest(project_path=project_path))
    if not isinstance(load_result, LoadProjectTemplateResultSuccess):
        logger.warning("Failed to load project template from %s: %s", project_path, load_result)
        return
    set_result = GriptapeNodes.handle_request(SetCurrentProjectRequest(project_id=load_result.project_id))
    if set_result.failed():
        logger.warning("Failed to set project as current: %s", set_result)
        return
    logger.info("Loaded and activated project template from %s", project_path)

def _set_config(libraries: list[str]) -> None:
    from griptape_nodes.retained_mode.griptape_nodes import GriptapeNodes  # noqa: PLC0415

    config_manager = GriptapeNodes.ConfigManager()
    config_manager.set_config_value(
        key="enable_workspace_file_watching",
        value=False,
    )
    config_manager.set_config_value(
        key="workspace_directory",
        value=str(output_dir),
    )
    # Set libraries_to_register LAST — this triggers library loading, and the
    # project template must already be active so that nodes which resolve
    # situations during init (e.g. save_node_output) see the overrides.
    config_manager.set_config_value(
        key="app_events.on_app_initialization_complete.libraries_to_register",
        value=libraries,
    )

# Load project template from the output directory (where we copied it)
# BEFORE libraries so that situations are available during node initialization.
_project_file = output_dir / "project.yml"
if _project_file.exists():
    _load_project_template(_project_file)

_set_config(LIBRARIES)

from deadline_cloud_workflow_executor import DeadlineCloudWorkflowExecutor
from griptape_nodes.drivers.storage.storage_backend import StorageBackend
from workflow import execute_workflow  # type: ignore[attr-defined]

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        default=None,
        help="Path to JSON file containing input for this task",
    )
    parser.add_argument(
        "--task-index",
        type=int,
        default=0,
        help="Index of the current task (0-based)",
    )
    parser.add_argument(
        "--pickle-control-flow-result",
        action="store_true",
        default={pickle_control_flow_result},
        help="Whether to pickle the control flow result",
    )

    args = parser.parse_args()
    input_file_path = args.input_file
    task_index = args.task_index
    pickle_result = args.pickle_control_flow_result

    logger.info("Starting task %d", task_index)

    try:
        if input_file_path:
            with open(input_file_path, 'r', encoding='utf-8') as f:
                flow_input = json.load(f)
            logger.info("Loaded input from file: %s", input_file_path)
        else:
            flow_input = {{}}
            logger.info("No input file provided, using empty input")
    except Exception as e:
        msg = f"Error reading JSON input file: {{e}}"
        logger.info(msg)
        raise

    project_file_path = output_dir / "project.yml"
    workflow_runner = DeadlineCloudWorkflowExecutor(
        storage_backend=StorageBackend("local"),
        project_file_path=project_file_path if project_file_path.exists() else None,
        skip_library_loading=True,
    )

    # Execute the workflow
    result = execute_workflow(
        input=flow_input,
        storage_backend="local",
        workflow_executor=workflow_runner,
        pickle_control_flow_result=pickle_result,
    )

    # Write output to task-specific file
    output_file = output_dir / f"output_{{task_index}}.json"

    # The result from execute_workflow contains the workflow output
    # We need to extract the relevant output values
    output_data = {{
        "task_index": task_index,
        "result": result,
    }}

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, default=str)

    logger.info("Task %d completed. Output written to: %s", task_index, output_file)
"""
