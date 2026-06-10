"""Multi-task publisher for AWS Deadline Cloud workflows.

This module handles publishing and executing a workflow as a single Deadline Cloud job
with multiple tasks, where each task processes one item from an input list.
"""

from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from botocore.exceptions import BotoCoreError, ClientError
from deadline.client.api import get_queue_user_boto3_session
from deadline.job_attachments.download import OutputDownloader
from deadline.job_attachments.models import Attachments, JobAttachmentS3Settings
from griptape_nodes.exe_types.node_types import BaseNode
from griptape_nodes.node_library.library_registry import LibraryNameAndVersion, LibraryRegistry
from griptape_nodes.retained_mode.events.os_events import (
    CopyFileRequest,
    CopyFileResultSuccess,
)
from griptape_nodes.retained_mode.events.workflow_events import (
    SaveWorkflowFileFromSerializedFlowRequest,
    SaveWorkflowFileFromSerializedFlowResultSuccess,
)
from griptape_nodes.retained_mode.griptape_nodes import GriptapeNodes
from publish import DEADLINE_CLOUD_LIBRARY_CONFIG_KEY
from publish.deadline_cloud_job_poller import DeadlineCloudJobPoller
from publish.deadline_cloud_multi_task_template_generator import DeadlineCloudMultiTaskJobTemplateGenerator
from publish.deadline_cloud_publisher import DeadlineCloudPublisher
from publish.utils import collect_metadata_sidecars, write_sidecar_output_files

if TYPE_CHECKING:
    from deadline.job_attachments.models import StorageProfile
    from griptape_nodes.retained_mode.events.flow_events import PackageNodesAsSerializedFlowResultSuccess

logger = logging.getLogger("deadline_cloud_multi_task_publisher")


@dataclass
class MultiTaskPublisherConfig:
    """Configuration for multi-task publisher."""

    workflow_name: str
    parameter_values_per_iteration: dict[int, dict[str, Any]]
    package_result: PackageNodesAsSerializedFlowResultSuccess
    farm_id: str
    queue_id: str
    storage_profile_id: str | None = None
    job_name: str | None = None
    job_description: str | None = None
    priority: int = 50
    initial_state: str = "READY"
    max_failed_tasks: int = 50
    max_task_retries: int = 10
    attachment_input_paths: list[str] | None = None
    attachment_output_paths: list[str] | None = None
    pickle_control_flow_result: bool = True
    host_requirements: dict[str, Any] | None = None
    result_parameter_name: str | None = None
    group_node_names: list[str] | None = None


class DeadlineCloudMultiTaskPublisher(DeadlineCloudPublisher):
    """Publisher for multi-task Deadline Cloud jobs.

    This publisher creates a single Deadline Cloud job with N tasks, where each task
    executes the same workflow with a different input from the items list.

    Extends DeadlineCloudPublisher to reuse common methods like library copying,
    job attachment processing, and environment file generation.
    """

    def __init__(self, config: MultiTaskPublisherConfig) -> None:
        """Initialize the multi-task publisher.

        Args:
            config: Configuration containing all necessary parameters
        """
        # Initialize parent with workflow name (we won't use its publish_workflow method)
        super().__init__(
            workflow_name=config.workflow_name,
            pickle_control_flow_result=config.pickle_control_flow_result,
        )
        self._multi_task_config = config
        self._job_bundle_path: Path | None = None
        self._output_dir_subdir: str = ""
        self._job_attachment_settings: JobAttachmentS3Settings | None = None

    @property
    def task_count(self) -> int:
        """Return the number of tasks to create."""
        return len(self._multi_task_config.parameter_values_per_iteration)

    async def publish_and_execute(self) -> list[Any]:
        """Publish the workflow and execute all tasks, returning aggregated results.

        Returns:
            List of results, one per task, in order of task index
        """
        workflow_file_path: Path | None = None
        try:
            # 1. Save the packaged workflow as a file
            logger.info("Saving packaged workflow for multi-task execution...")
            workflow_file_result = await self._save_workflow_file()
            workflow_file_path = Path(workflow_file_result.file_path)
            logger.info("Workflow saved to: %s", workflow_file_path)

            # 2. Package the workflow as a job bundle
            logger.info("Packaging workflow for multi-task execution...")
            package_path = self._package_multi_task_workflow(workflow_file_path)
            self._job_bundle_path = Path(package_path)
            logger.info("Workflow packaged to path: %s", package_path)

            # 3. Generate input files for each task
            logger.info("Generating input files for %d tasks...", self.task_count)
            self._generate_task_input_files(package_path)

            # 4. Process job attachments
            logger.info("Processing job attachments...")
            self._job_attachment_settings, attachments = self._process_multi_task_job_attachments(package_path)
            logger.info("Job attachments processed successfully")

            # 5. Submit the job
            logger.info("Submitting multi-task job to Deadline Cloud...")
            job_id = self._submit_multi_task_job(
                package_path=package_path,
                attachments=attachments,
            )
            self._job_id = job_id
            logger.info("Job submitted successfully: %s", job_id)

            # 6. Monitor job until completion using the poller
            logger.info("Monitoring job execution...")
            self._wait_for_job_completion(job_id)
            logger.info("Job completed: %s", job_id)

            # 7. Collect results
            logger.info("Collecting task results...")
            results = self._collect_results(job_id)
            logger.info("Collected %d results", len(results))

            return results  # noqa: TRY300

        except (ClientError, BotoCoreError) as e:
            details = f"AWS API error during multi-task execution: {e}"
            logger.error(details)
            raise RuntimeError(details) from e
        except Exception as e:
            details = f"Unexpected error during multi-task execution: {e}"
            logger.exception(details)
            raise RuntimeError(details) from e
        finally:
            if workflow_file_path is not None:
                workflow_file_path.unlink(missing_ok=True)

    async def _save_workflow_file(self) -> SaveWorkflowFileFromSerializedFlowResultSuccess:
        """Save the packaged workflow as a runnable workflow file.

        Returns:
            Result containing the path to the saved workflow file
        """
        sanitized_name = self._multi_task_config.workflow_name.replace(" ", "_")
        file_name = f"{sanitized_name}_multi_task_workflow"

        workflow_file_request = SaveWorkflowFileFromSerializedFlowRequest(
            file_name=file_name,
            serialized_flow_commands=self._multi_task_config.package_result.serialized_flow_commands,
            workflow_shape=self._multi_task_config.package_result.workflow_shape,
            pickle_control_flow_result=self._multi_task_config.pickle_control_flow_result,
        )

        workflow_result = await GriptapeNodes.ahandle_request(workflow_file_request)
        if not isinstance(workflow_result, SaveWorkflowFileFromSerializedFlowResultSuccess):
            msg = f"Failed to save workflow file: {workflow_result.result_details}"
            raise RuntimeError(msg)  # noqa: TRY004

        return workflow_result

    def _get_libraries_from_serialized_flow(self) -> list[LibraryNameAndVersion]:
        """Extract library references from the serialized flow commands.

        Returns:
            List of LibraryNameAndVersion objects for all libraries used in the flow
        """
        serialized_flow = self._multi_task_config.package_result.serialized_flow_commands

        # Get libraries from node_dependencies
        return list(serialized_flow.node_dependencies.libraries)

    def _package_multi_task_workflow(self, workflow_file_path: Path) -> str:  # noqa: PLR0915
        """Package the workflow as a Deadline Cloud job bundle for multi-task execution.

        Args:
            workflow_file_path: Path to the saved workflow .py file

        Returns:
            Path to the job bundle directory
        """
        config_manager = GriptapeNodes.ConfigManager()
        secrets_manager = GriptapeNodes.get_instance()._secrets_manager

        # Create temporary directory for packaging
        temp_dir = Path(tempfile.mkdtemp(prefix=f"{self._multi_task_config.workflow_name}_multi_task_bundle_"))
        job_bundle_dir = temp_dir
        assets_dir = job_bundle_dir / "assets"
        inputs_dir = assets_dir / "inputs"

        # Create bundle directory structure
        assets_dir.mkdir(parents=True)
        inputs_dir.mkdir(parents=True)

        try:
            local_publish_path = Path(__file__).parent

            # 1. Copy the workflow file
            copy_workflow_result = GriptapeNodes.handle_request(
                CopyFileRequest(
                    source_path=str(workflow_file_path),
                    destination_path=str(assets_dir / "workflow.py"),
                )
            )
            if not isinstance(copy_workflow_result, CopyFileResultSuccess):
                details = f"Failed to copy workflow file from '{workflow_file_path}' to '{assets_dir / 'workflow.py'}'."
                logger.error(details)
                raise TypeError(details)  # noqa: TRY301

            # 2. Copy supporting files
            init_file_path = local_publish_path / "__init__.py"
            copy_init_result = GriptapeNodes.handle_request(
                CopyFileRequest(
                    source_path=str(init_file_path),
                    destination_path=str(assets_dir / "__init__.py"),
                )
            )
            if not isinstance(copy_init_result, CopyFileResultSuccess):
                details = f"Failed to copy '{init_file_path}' to '{assets_dir / '__init__.py'}'."
                logger.error(details)
                raise TypeError(details)  # noqa: TRY301

            deadline_workflow_executor_file_path = local_publish_path / "deadline_cloud_workflow_executor.py"
            copy_executor_result = GriptapeNodes.handle_request(
                CopyFileRequest(
                    source_path=str(deadline_workflow_executor_file_path),
                    destination_path=str(assets_dir / "deadline_cloud_workflow_executor.py"),
                )
            )
            if not isinstance(copy_executor_result, CopyFileResultSuccess):
                details = f"Failed to copy '{deadline_workflow_executor_file_path}' to '{assets_dir / 'deadline_cloud_workflow_executor.py'}'."
                logger.error(details)
                raise TypeError(details)  # noqa: TRY301

            # 3. Copy libraries - use the same method as parent class
            node_libraries = self._get_libraries_from_serialized_flow()
            runtime_env_path = Path("{{Param.LocationToRemap}}/assets/libraries")

            # Create a mock workflow object to pass to the parent's method
            # We need to pass the library references we extracted
            library_paths = self._copy_libraries_to_path_for_multi_task(
                node_libraries=node_libraries,
                destination_path=assets_dir / "libraries",
                runtime_env_path=runtime_env_path,
            )

            # 4. Create configuration
            config = config_manager.user_config.copy()
            config["workspace_directory"] = "."
            config["app_events"] = {
                "on_app_initialization_complete": {
                    "workflows_to_register": [],
                    "libraries_to_register": library_paths,
                }
            }

            with (assets_dir / "griptape_nodes_config.json").open("w", encoding="utf-8") as config_file:
                json.dump(config, config_file, indent=2)

            # 5. Create environment file - reuse parent's method
            env_file_mapping = self._get_merged_env_file_mapping(secrets_manager.workspace_env_path)
            self._write_env_file(assets_dir / ".env", env_file_mapping)

            # 6. Create requirements.txt
            engine_version = self._get_engine_version_string()
            source, commit_id = self._get_install_source()
            if source == "git" and commit_id is not None:
                engine_version = commit_id

            with (assets_dir / "requirements.txt").open("w", encoding="utf-8") as req_file:
                req_file.write(
                    f"griptape-nodes @ git+https://github.com/griptape-ai/griptape-nodes.git@{engine_version}\n"
                )
                for library_ref in node_libraries:
                    lib = LibraryRegistry.get_library(library_ref.library_name)
                    library_data = lib.get_library_data()
                    deps = library_data.metadata.dependencies
                    if deps and deps.pip_dependencies:
                        if deps.pip_install_flags:
                            req_file.write(f"{' '.join(deps.pip_install_flags)}\n")
                        for dep in deps.pip_dependencies:
                            if dep.startswith("-e"):
                                continue
                            req_file.write(f"{dep}\n")
                    else:
                        # Fallback: check for a sibling library JSON with deps
                        # (handles no-deps variants used for local dev)
                        self._write_deps_from_sibling_json(library_ref.library_name, req_file)

            # 7. Gather and copy static file dependencies from FileSelector nodes
            file_selector_nodes = self._gather_file_selector_nodes()
            if file_selector_nodes:
                self._resolve_and_copy_static_files(file_selector_nodes, assets_dir)

            # 8. Write Deadline-specific project template (always, even without FileSelector nodes)
            self._write_deadline_project_template(assets_dir)

            # 9. Generate Multi-Task Job Template
            self._job_template = DeadlineCloudMultiTaskJobTemplateGenerator.generate_job_template(
                job_bundle_dir,
                self._multi_task_config.workflow_name,
                library_paths,
                task_count=self.task_count,
                pickle_control_flow_result=self._multi_task_config.pickle_control_flow_result,
                host_requirements=self._multi_task_config.host_requirements,
            )

            logger.info("Multi-task job bundle created at: %s", job_bundle_dir)
            return str(job_bundle_dir)

        except Exception as e:
            details = f"Error packaging workflow: {e}"
            logger.exception(details)
            raise RuntimeError(details) from e

    def _copy_libraries_to_path_for_multi_task(
        self,
        node_libraries: list[LibraryNameAndVersion],
        destination_path: Path,
        runtime_env_path: Path,
    ) -> list[str]:
        """Copy libraries to the destination path for the workflow.

        This is similar to the parent's _copy_libraries_to_path_for_workflow but
        doesn't require a Workflow object.

        Args:
            node_libraries: List of library references to copy
            destination_path: Where to copy the libraries
            runtime_env_path: Runtime path for library references

        Returns:
            List of library paths for the runtime environment
        """
        import os

        from griptape_nodes.retained_mode.events.os_events import CopyTreeRequest, CopyTreeResultSuccess

        library_paths: list[str] = []

        for library_ref in node_libraries:
            library = GriptapeNodes.LibraryManager().get_library_info_by_library_name(library_ref.library_name)

            if library is None:
                logger.warning(
                    "Could not find library info for '%s', skipping",
                    library_ref.library_name,
                )
                continue

            library_data = LibraryRegistry.get_library(library_ref.library_name).get_library_data()

            if library.library_path.endswith(".json"):
                library_path = Path(library.library_path)
                absolute_library_path = library_path.resolve()
                abs_paths = [absolute_library_path]
                for node in library_data.nodes:
                    p = (library_path.parent / Path(node.file_path)).resolve()
                    abs_paths.append(p)
                common_root = Path(os.path.commonpath([str(p) for p in abs_paths]))
                dest = destination_path / common_root.name
                copy_tree_request = CopyTreeRequest(
                    source_path=str(common_root),
                    destination_path=str(dest),
                    ignore_patterns=[".venv", "__pycache__"],
                    dirs_exist_ok=True,
                )
                copy_tree_result = GriptapeNodes.handle_request(copy_tree_request)
                if not isinstance(copy_tree_result, CopyTreeResultSuccess):
                    logger.warning(
                        "Failed to copy library files from '%s' to '%s' for library '%s'",
                        common_root,
                        dest,
                        library_ref.library_name,
                    )
                    continue
                library_path_relative_to_common_root = absolute_library_path.relative_to(common_root)
                library_paths.append(
                    (runtime_env_path / common_root.name / library_path_relative_to_common_root).as_posix()
                )
            else:
                library_paths.append(library.library_path)

        return library_paths

    def _get_engine_version_string(self) -> str:
        """Get the Griptape Nodes engine version as a string."""
        from griptape_nodes.retained_mode.events.app_events import (
            GetEngineVersionRequest,
            GetEngineVersionResultSuccess,
        )

        result = GriptapeNodes.handle_request(GetEngineVersionRequest())
        if isinstance(result, GetEngineVersionResultSuccess):
            return f"v{result.major}.{result.minor}.{result.patch}"
        return "main"

    def _generate_task_input_files(self, package_path: str) -> None:
        """Generate input JSON files for each task.

        The input format matches what the workflow executor expects:
        {start_node_name: {param_name: param_value, ...}}

        Args:
            package_path: Path to the job bundle directory
        """
        inputs_dir = Path(package_path) / "assets" / "inputs"
        inputs_dir.mkdir(parents=True, exist_ok=True)

        # Get the StartFlow node name from the package result's parameter_name_mappings
        # Index 0 contains the Start node mappings with the actual node name
        start_node_name = self._multi_task_config.package_result.parameter_name_mappings[0].node_name

        for iteration_index, parameter_values in self._multi_task_config.parameter_values_per_iteration.items():
            # Format input as expected by the workflow executor
            flow_input = {start_node_name: parameter_values}

            input_file = inputs_dir / f"input_{iteration_index}.json"
            with input_file.open("w", encoding="utf-8") as f:
                json.dump(flow_input, f, indent=2, default=str)
            logger.debug("Generated input file: %s", input_file)

    def _gather_models_for_group(self) -> list[str]:
        """Gather HuggingFace model names from the group's child nodes.

        This method checks each node in the group for parameters that match
        known HuggingFace repository names in the local cache.

        Returns:
            List of HuggingFace model repository names used by nodes in the group
        """
        group_node_names = self._multi_task_config.group_node_names
        if not group_node_names:
            logger.info("No group node names provided, skipping model gathering")
            return []

        huggingface_repo_names = self._get_huggingface_repo_names()
        if not huggingface_repo_names:
            logger.info("No HuggingFace repos found in cache, skipping model gathering")
            return []

        object_manager = GriptapeNodes.ObjectManager()
        models: list[str] = []
        for node_name in group_node_names:
            node = object_manager.get_object_by_name(node_name)
            if node is None:
                logger.warning("Node '%s' not found, skipping model gathering for it", node_name)
                continue
            if isinstance(node, BaseNode):
                models.extend(self._get_model_parameters_for_node(node, huggingface_repo_names))

        logger.info("Found %d model(s) in group nodes: %s", len(models), models)
        return models

    def _gather_file_selector_nodes(self) -> list[tuple[str, str]]:
        """Find FileSelector nodes within the group's child nodes.

        Overrides the parent method which searches the top-level flow. For multi-task
        publishing, we need to search the nodes inside the group instead.

        Returns:
            List of (node_name, macro_string) tuples for each FileSelector node
            that has a non-empty selected_file parameter value.
        """
        from griptape_nodes.retained_mode.events.parameter_events import (
            GetParameterValueRequest,
            GetParameterValueResultSuccess,
        )
        from publish.deadline_cloud_publisher import (
            FILE_SELECTOR_LIBRARY_NAME,
            SELECT_FROM_PROJECT_NODE_TYPE,
            SELECT_FROM_PROJECT_PARAM_NAME,
        )

        group_node_names = self._multi_task_config.group_node_names
        if not group_node_names:
            return []

        object_manager = GriptapeNodes.ObjectManager()
        file_selector_nodes: list[tuple[str, str]] = []

        for node_name in group_node_names:
            node = object_manager.get_object_by_name(node_name)
            if node is None or not isinstance(node, BaseNode):
                continue

            if (
                node.metadata.get("library") != FILE_SELECTOR_LIBRARY_NAME
                or node.metadata.get("node_type") != SELECT_FROM_PROJECT_NODE_TYPE
            ):
                continue

            get_param_value_result = GriptapeNodes.handle_request(
                GetParameterValueRequest(parameter_name=SELECT_FROM_PROJECT_PARAM_NAME, node_name=node.name)
            )
            if not isinstance(get_param_value_result, GetParameterValueResultSuccess):
                logger.warning(
                    "FileSelector node '%s' has no '%s' parameter value, skipping.",
                    node.name,
                    SELECT_FROM_PROJECT_PARAM_NAME,
                )
                continue

            param_value = get_param_value_result.value
            if not param_value:
                logger.warning(
                    "FileSelector node '%s' has empty or non-string '%s' value, skipping.",
                    node.name,
                    SELECT_FROM_PROJECT_PARAM_NAME,
                )
                continue

            file_selector_nodes.append((node.name, param_value))
            logger.info("Found FileSelector node '%s' with file_path: %s", node.name, param_value)

        return file_selector_nodes

    def _process_multi_task_job_attachments(self, package_path: str) -> tuple[JobAttachmentS3Settings, Attachments]:
        """Process and upload job attachments for multi-task job.

        Args:
            package_path: Path to the job bundle directory

        Returns:
            Tuple of (job_attachment_settings, attachments)
        """
        farm_id = self._multi_task_config.farm_id
        queue_id = self._multi_task_config.queue_id
        storage_profile_id = self._multi_task_config.storage_profile_id

        # Check if model attachments are enabled
        enable_models_as_attachments = self._get_config_value(
            DEADLINE_CLOUD_LIBRARY_CONFIG_KEY, "enable_models_as_attachments", default=True
        )

        deadline_client = self._get_client()

        # Get queue information for job attachment settings
        queue_response = deadline_client.get_queue(farmId=farm_id, queueId=queue_id)
        job_attachment_settings = JobAttachmentS3Settings(**queue_response["jobAttachmentSettings"])

        queue_session = get_queue_user_boto3_session(
            deadline_client,
            None,
            farm_id,
            queue_id,
        )
        queue_session._session.set_config_variable("region", self._session.region_name)

        storage_profile: StorageProfile | None = self._get_storage_profile_for_queue(
            farm_id, queue_id, storage_profile_id
        )

        # Collect input paths
        job_bundle_path = Path(package_path)
        assets_dir = job_bundle_path / "assets"
        bundle_input_paths = []

        if assets_dir.is_dir():
            bundle_input_paths.extend(self.expand_directories_to_files([str(assets_dir)]))

        # Add any additional attachment paths from config
        if self._multi_task_config.attachment_input_paths:
            bundle_input_paths.extend(self.expand_directories_to_files(self._multi_task_config.attachment_input_paths))

        logger.info("Uploading %d job bundle files", len(bundle_input_paths))
        attachments = self.upload_paths_as_job_attachments(
            input_paths=bundle_input_paths,
            output_paths=[str(job_bundle_path / "output")],
            farm_id=farm_id,
            queue_id=queue_id,
            storage_profile=storage_profile,
            job_attachment_settings=job_attachment_settings,
            queue_session=queue_session,
        )

        # Upload model files separately (if enabled)
        if enable_models_as_attachments:
            model_names = self._gather_models_for_group()
            model_paths = self._get_model_paths(model_names)
            if model_paths:
                logger.info("Uploading %d model files", len(model_paths))
                model_attachments = self.upload_paths_as_job_attachments(
                    input_paths=model_paths,
                    output_paths=[],
                    farm_id=farm_id,
                    queue_id=queue_id,
                    storage_profile=storage_profile,
                    job_attachment_settings=job_attachment_settings,
                    queue_session=queue_session,
                )
                attachments.manifests.extend(model_attachments.manifests)
                logger.info("Added %d model manifest(s)", len(model_attachments.manifests))

        return job_attachment_settings, attachments

    def _submit_multi_task_job(
        self,
        package_path: str,
        attachments: Attachments,
    ) -> str:
        """Submit the multi-task job to Deadline Cloud.

        Args:
            package_path: Path to the job bundle directory
            attachments: Uploaded attachment manifests

        Returns:
            Job ID of the submitted job
        """
        deadline_client = self._get_client()

        job_bundle_path = Path(package_path)

        # Use the job template dict that was stored during packaging
        job_template = self._job_template

        # Update job template with name and description (these belong in the template, not the API request)
        if self._multi_task_config.job_name:
            job_template["name"] = self._multi_task_config.job_name
        if self._multi_task_config.job_description:
            job_template["description"] = self._multi_task_config.job_description

        template_content = json.dumps(job_template)

        # Build job parameters
        job_parameters = {
            "DataDir": {
                "path": str(job_bundle_path),
            },
            "LocationToRemap": {
                "path": str(job_bundle_path),
            },
            "ModelsLocationToRemap": {
                "path": self._get_huggingface_cache_dir(),
            },
            "TaskCount": {
                "int": str(self.task_count),
            },
            "OutputDir": {
                "string": self._multi_task_config.workflow_name.replace(" ", "_"),
            },
        }

        # Build create job request
        create_job_request: dict[str, Any] = {
            "farmId": self._multi_task_config.farm_id,
            "queueId": self._multi_task_config.queue_id,
            "template": template_content,
            "templateType": "JSON",
            "parameters": job_parameters,
            "priority": self._multi_task_config.priority,
            "targetTaskRunStatus": self._multi_task_config.initial_state,
            "maxFailedTasksCount": self._multi_task_config.max_failed_tasks,
            "maxRetriesPerTask": self._multi_task_config.max_task_retries,
        }

        # Add attachments
        if attachments and attachments.manifests:
            create_job_request["attachments"] = attachments.to_dict()

        response = deadline_client.create_job(**create_job_request)
        return response["jobId"]

    def _wait_for_job_completion(self, job_id: str) -> None:
        """Wait for the job to complete using the job poller.

        Args:
            job_id: ID of the job to monitor
        """
        deadline_client = self._get_client()
        config = self._get_config_parser()

        poller = DeadlineCloudJobPoller(
            client=deadline_client,
            config=config,
            job_id=job_id,
            queue_id=self._multi_task_config.queue_id,
            farm_id=self._multi_task_config.farm_id,
            max_poll_interval=10,
            status_callback=self._job_status_callback,
        )

        poller.poll_job()

    def _job_status_callback(self, job_details: Any, elapsed_time: float) -> None:
        """Callback for job status updates.

        Args:
            job_details: Current job details from poller
            elapsed_time: Time elapsed since polling started
        """
        logger.info(
            "Job %s - Status: %s, Task Status: %s (elapsed: %.1fs)",
            job_details.job_id,
            job_details.lifecycle_status,
            job_details.task_run_status,
            elapsed_time,
        )

    def _collect_results(self, job_id: str) -> list[Any]:
        """Collect results from all completed tasks.

        For multi-task jobs, each task writes a workflow_output.json file.
        We download these from the job attachments output location.

        Args:
            job_id: ID of the completed job

        Returns:
            List of results, one per task, in order of task index
        """
        logger.info("Result collection for job %s - downloading from job attachments", job_id)

        farm_id = self._multi_task_config.farm_id
        queue_id = self._multi_task_config.queue_id

        deadline_client = self._get_client()

        # Get job attachment settings if not already cached
        if self._job_attachment_settings is None:
            queue_response = deadline_client.get_queue(farmId=farm_id, queueId=queue_id)
            self._job_attachment_settings = JobAttachmentS3Settings(**queue_response["jobAttachmentSettings"])

        queue_session = get_queue_user_boto3_session(
            deadline_client,
            None,
            farm_id,
            queue_id,
        )

        downloader = OutputDownloader(
            s3_settings=self._job_attachment_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
            session=queue_session,
        )

        def on_download_progress(progress: Any) -> bool:
            logger.info("Downloading output: %s", progress)
            return True  # Continue downloading

        try:
            downloader.download_job_output(
                on_downloading_files=on_download_progress,
            )
            logger.info("Job output downloaded successfully.")

            output_paths_by_root = downloader.get_output_paths_by_root()
            return self._extract_multi_task_results(output_paths_by_root)
        except Exception as e:
            details = f"Error downloading job output: {e}"
            logger.error(details)
            raise RuntimeError(details) from e

    def _get_static_files_directory(self) -> Path:
        """Get the static files directory path."""
        workspace_dir = GriptapeNodes.ConfigManager().get_config_value("workspace_directory")
        static_files_dir = GriptapeNodes.ConfigManager().get_config_value("static_files_directory")
        return Path(workspace_dir) / static_files_dir

    def _copy_static_files(self, output_paths_by_root: dict[str, list[str]]) -> None:
        """Copy static files from output to the static files directory.

        Args:
            output_paths_by_root: Dictionary mapping root paths to output file paths
        """
        output_dir_subdir = self._get_output_dir_subdir()
        output_segment = f"output/{output_dir_subdir}"
        static_dir = self._get_static_files_directory()
        static_dir.mkdir(parents=True, exist_ok=True)

        for root_path, output_files in output_paths_by_root.items():
            for output_file in output_files:
                if "staticfiles" in output_file and output_file.startswith(output_segment):
                    static_file_path = Path(root_path) / output_file
                    if static_file_path.exists():
                        try:
                            staticfiles_idx = output_file.index("staticfiles/")
                            relative_in_static = output_file[staticfiles_idx + len("staticfiles/") :]
                            dest_file = static_dir / relative_in_static
                            dest_file.parent.mkdir(parents=True, exist_ok=True)
                            copy_result = GriptapeNodes.handle_request(
                                CopyFileRequest(
                                    source_path=str(static_file_path),
                                    destination_path=str(dest_file),
                                )
                            )
                            if not isinstance(copy_result, CopyFileResultSuccess):
                                logger.warning("Failed to copy static file %s to %s", output_file, dest_file)
                        except ValueError as e:
                            logger.warning("Failed to copy static file %s: %s", output_file, e)

    def _get_output_dir_subdir(self) -> str:
        """Get the output directory subdirectory name."""
        return self._multi_task_config.workflow_name.replace(" ", "_")

    def _translate_worker_paths_to_local(  # noqa: C901
        self, value: Any, output_paths_by_root: dict[str, list[str]]
    ) -> Any:
        """Translate Deadline worker paths in a value to local filesystem paths.

        Worker paths look like: /sessions/session-.../assetroot-.../output/<subdir>/...
        These need to be translated to local paths like: /local/root/output/<subdir>/...

        Args:
            value: The value that may contain worker paths (can be str, dict, list, or other).
            output_paths_by_root: A mapping of local root paths to their relative output file paths.

        Returns:
            The value with worker paths translated to local paths.
        """
        output_dir_subdir = self._get_output_dir_subdir()
        output_segment = f"output/{output_dir_subdir}"

        # Find the local root path that contains our output directory
        local_output_path: str | None = None
        for local_root, output_files in output_paths_by_root.items():
            for output_file in output_files:
                if output_file.startswith(output_segment):
                    local_output_path = str(Path(local_root) / output_segment)
                    break
            if local_output_path:
                break

        if not local_output_path:
            # No matching output path found, return unchanged
            return value

        def translate_value(val: Any) -> Any:
            """Recursively translate paths in a value."""
            if isinstance(val, str):
                # Check if this string contains the output segment from a worker path
                if output_segment in val:
                    # Find where the output segment starts and replace everything before it
                    segment_idx = val.find(output_segment)
                    if segment_idx != -1:
                        # Get the suffix after the output segment base
                        suffix = val[segment_idx + len(output_segment) :]
                        return local_output_path + suffix
                return val
            if isinstance(val, dict):
                return {k: translate_value(v) for k, v in val.items()}
            if isinstance(val, list):
                return [translate_value(item) for item in val]
            return val

        return translate_value(value)

    def _extract_multi_task_results(self, output_paths_by_root: dict[str, list[str]]) -> list[Any]:  # noqa: C901
        """Extract results from downloaded output files for each task.

        Each task writes its output to output_X.json where X is the task index.
        The JSON structure is: {"task_index": X, "result": {end_node_name: {param_name: value, ...}}}

        This method also:
        - Copies any static files to the static files directory
        - Translates worker paths in results to local filesystem paths

        Args:
            output_paths_by_root: Dictionary mapping root paths to output file paths

        Returns:
            List of results, one per task, in order of task index
        """
        import re

        # Copy any static files to the static files directory
        self._copy_static_files(output_paths_by_root)

        # Initialize results list with None for each task
        results: list[Any] = [None] * self.task_count

        # Get the result parameter name from config (maps to new_item_to_add)
        result_param_name = self._multi_task_config.result_parameter_name

        # Find and parse output_X.json files
        for root_path, output_files in output_paths_by_root.items():
            for output_file in output_files:
                # Match output_X.json pattern
                match = re.match(r"output_(\d+)\.json$", Path(output_file).name)
                if match:
                    task_index = int(match.group(1))
                    output_file_path = Path(root_path) / output_file
                    try:
                        with output_file_path.open(encoding="utf-8") as f:
                            task_output = json.load(f)

                        # Extract the result value
                        # File structure: {"task_index": X, "result": {end_node_name: {param_name: value}}}
                        task_result = task_output.get("result", {})

                        # If we have a specific parameter to extract, find it
                        if result_param_name:
                            # The result dict has end_node_name as key, then params inside
                            extracted_value = None
                            for end_node_params in task_result.values():
                                if isinstance(end_node_params, dict) and result_param_name in end_node_params:
                                    extracted_value = end_node_params[result_param_name]
                                    break
                            # Translate worker paths to local paths in the extracted value
                            if extracted_value is not None:
                                extracted_value = self._translate_worker_paths_to_local(
                                    extracted_value, output_paths_by_root
                                )
                            if task_index < self.task_count:
                                results[task_index] = extracted_value
                        elif task_index < self.task_count:
                            # No specific parameter, return the full result with translated paths
                            results[task_index] = self._translate_worker_paths_to_local(
                                task_result, output_paths_by_root
                            )

                        # Clean up the downloaded file
                        output_file_path.unlink(missing_ok=True)
                        logger.debug("Extracted result for task %d from %s", task_index, output_file)

                    except Exception as e:
                        logger.warning("Failed to parse task output file %s: %s", output_file_path, e)

        # Write any macro-referenced output files to the correct local project locations.
        # Collect metadata sidecars once (shared across all tasks).
        downloaded_files = self._build_downloaded_files_lookup(output_paths_by_root)
        metadata_sidecars = collect_metadata_sidecars(downloaded_files)
        sidecar_written = write_sidecar_output_files(downloaded_files, metadata_sidecars)

        for task_result in results:
            if task_result is not None:
                self._write_macro_output_files(task_result, downloaded_files, sidecar_written)

        logger.info("Extracted results for %d/%d tasks", len([r for r in results if r is not None]), self.task_count)
        return results

    def _build_downloaded_files_lookup(self, output_paths_by_root: dict[str, list[str]]) -> dict[str, Path]:
        """Build a lookup of relative paths within the output dir to their full local paths.

        Args:
            output_paths_by_root: A mapping of local root paths to their relative output file paths.

        Returns:
            Map of relative paths within the output dir to their full local paths.
        """
        output_dir_subdir = self._get_output_dir_subdir()
        output_segment = f"output/{output_dir_subdir}"

        downloaded_files: dict[str, Path] = {}
        for local_root, output_files in output_paths_by_root.items():
            for output_file in output_files:
                if output_file.startswith(output_segment):
                    relative_in_output = output_file[len(output_segment) + 1 :]
                    downloaded_files[relative_in_output] = Path(local_root) / output_file
        return downloaded_files

    def _write_macro_output_files(
        self,
        task_result: Any,
        downloaded_files: dict[str, Path],
        sidecar_written: set[str],
    ) -> None:
        """Write macro-referenced output files to the correct local project locations.

        Walks the task result looking for parameter values that contain macro strings
        (e.g., "{outputs}/image.png"). Skips files already written via sidecar-based
        saving and falls back to direct macro resolution for the rest.

        Args:
            task_result: The task result (already path-translated).
            downloaded_files: Map of relative paths to their downloaded local paths.
            sidecar_written: Set of relative paths already written via sidecar.
        """

        def visit_value(value: Any) -> None:
            """Recursively visit values and write any macro-referenced files."""
            if isinstance(value, str) and "{" in value and "}" in value:
                self._try_write_macro_file(value, downloaded_files, sidecar_written)
            elif isinstance(value, dict):
                for v in value.values():
                    visit_value(v)
            elif isinstance(value, list):
                for item in value:
                    visit_value(item)

        visit_value(task_result)

    def _try_write_macro_file(
        self,
        macro_string: str,
        downloaded_files: dict[str, Path],
        sidecar_written: set[str],
    ) -> None:
        """Try to write a macro-referenced file to the correct local project location.

        Skips files that were already written via sidecar-based saving.
        Falls back to resolving the macro directly for files without sidecars.

        Args:
            macro_string: A macro path string like "{outputs}/image.png".
            downloaded_files: Map of relative paths to their downloaded local paths.
            sidecar_written: Set of relative paths already written via sidecar.
        """
        from griptape_nodes.common.macro_parser import ParsedMacro
        from griptape_nodes.files.file import File, FileWriteError
        from griptape_nodes.retained_mode.events.project_events import (
            GetPathForMacroRequest,
            GetPathForMacroResultSuccess,
        )

        try:
            parsed = ParsedMacro(macro_string)
            if not parsed.get_variables():
                return

            result = GriptapeNodes.handle_request(GetPathForMacroRequest(parsed_macro=parsed, variables={}))
            if not isinstance(result, GetPathForMacroResultSuccess):
                return
        except Exception:
            return

        # Skip if already written via sidecar
        relative_path = str(result.resolved_path)
        if relative_path in sidecar_written:
            return

        # Find the corresponding downloaded file
        source_path = downloaded_files.get(relative_path)
        if source_path is None or not source_path.exists():
            return

        try:
            file_bytes = source_path.read_bytes()
            local_path = File(macro_string).write_bytes(file_bytes)
        except FileWriteError as e:
            logger.warning("Failed to write macro output '%s': %s", macro_string, e)
        else:
            logger.info("Wrote macro output '%s' -> '%s'", macro_string, local_path)
