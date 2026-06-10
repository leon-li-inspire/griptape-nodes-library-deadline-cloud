import logging
from typing import Any

from griptape_nodes.exe_types.core_types import Parameter, ParameterGroup, ParameterMessage, ParameterMode
from griptape_nodes.exe_types.node_types import BaseNode
from publish.base_deadline_cloud import BaseDeadlineCloud
from publish.deadline_cloud_resource_options import DeadlineCloudResourceOptions

logger = logging.getLogger(__name__)


class DeadlineCloudJobSubmissionConfigAdvancedParameter(BaseDeadlineCloud):
    def __init__(
        self, node: BaseNode, metadata: dict[Any, Any] | None = None, allowed_modes: set[ParameterMode] | None = None
    ) -> None:
        BaseDeadlineCloud.__init__(self, session=BaseDeadlineCloud._get_session())
        self.node = node
        if metadata is None:
            metadata = {}

        farm_id = metadata.get(
            "farm_id",
            BaseDeadlineCloud._get_default_farm_id(),
        )
        queue_id = metadata.get(
            "queue_id",
            BaseDeadlineCloud._get_default_queue_id(),
        )
        self.updating_queue_id_lock = False
        storage_profile_id = metadata.get(
            "storage_profile_id",
            BaseDeadlineCloud._get_default_storage_profile_id(),
        )

        try:
            self.list_farms(raise_on_error=True)
        except Exception:
            msg = "Failed to validate Deadline Cloud credentials. Please ensure that your AWS credentials are configured correctly."
            msg = (
                "Failed to create the Deadline Cloud client.\n\n"
                f"Error details: {msg}\n\n"
                "Please:\n"
                "   1. Ensure that your AWS profile name is configured correctly in the Library settings\n"
                "   2. Ensure that your AWS credentials are valid, refreshed, and have the necessary permissions\n"
                "   3. Refresh the libraries in the Griptape UI\n"
            )
            parameter_message = ParameterMessage(
                name="deadline_cloud_credentials_parameter_message",
                title="Deadline Cloud Credentials Configuration Warning",
                variant="warning",
                value=msg,
            )
            self.node.add_node_element(parameter_message)
            self.node.move_element_to_position(element=parameter_message.name, position=0)

        # Add advanced job config group
        with ParameterGroup(name="Job Submission Config Advanced") as job_submission_config_group_advanced:
            Parameter(
                name="priority",
                input_types=["int"],
                type="int",
                output_type="int",
                default_value=50,
                tooltip="The job priority for the Deadline Cloud Job.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="initial_state",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value="READY",
                tooltip="The initial state for the Deadline Cloud Job.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="max_failed_tasks",
                input_types=["int"],
                type="int",
                output_type="int",
                default_value=50,
                tooltip="The maximum number of failed tasks before the job is considered failed.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="max_task_retries",
                input_types=["int"],
                type="int",
                output_type="int",
                default_value=10,
                tooltip="The maximum number of task retries before the job is considered failed.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="farm_id",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value=farm_id,
                traits={
                    DeadlineCloudResourceOptions(
                        choices=list(self._get_farm_choices_values_map().keys()),
                        choices_value_lookup=self._get_farm_choices_values_map(),
                        value_field="farmId",
                    )
                },
                tooltip="The farm to use for the Deadline Cloud Job.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="queue_id",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value=queue_id,
                traits={self._get_queue_id_options(farm_id=farm_id)},
                tooltip="The queue to use for the Deadline Cloud Job.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="storage_profile_id",
                input_types=["str"],
                type="str",
                output_type="str",
                default_value=storage_profile_id,
                traits={self._get_storage_profile_id_options(farm_id=farm_id, queue_id=queue_id)},
                tooltip="The storage profile ID for the Deadline Cloud Job.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="conda_channels",
                input_types=["str"],
                type="str",
                default_value="conda-forge",
                output_type="str",
                tooltip="Conda channels to install packages from.",
                allowed_modes=allowed_modes,
            )
            Parameter(
                name="conda_packages",
                input_types=["str"],
                type="str",
                default_value="python=3.12 git",
                output_type="str",
                tooltip="Conda packages install job.",
                allowed_modes=allowed_modes,
            )

        job_submission_config_group_advanced.ui_options = {"hide": False, "collapsed": True}
        self.node.add_node_element(job_submission_config_group_advanced)

    def _get_default_choice(self, choices_map: dict, default_value: Any) -> str:
        """Get the default choice key from a choices map, falling back to first key if default not found."""
        if default_value in choices_map.values():
            return next(key for key, val in choices_map.items() if val == default_value)
        return next(iter(choices_map.keys())) if choices_map else ""

    def after_value_set(self, parameter: Parameter, value: Any) -> None:
        if parameter.name == "farm_id":
            queue_choices = self._get_queue_choices_values_map(farm_id=value)
            queue_default = self._get_default_choice(queue_choices, BaseDeadlineCloud._get_default_queue_id())
            # Lock to prevent cascading updates
            self.updating_queue_id_lock = True
            self._update_option_choices(
                param="queue_id",
                choices=list(queue_choices.keys()),
                choices_value_lookup=queue_choices,
                default=queue_default,
            )
            new_queue_id = queue_choices.get(queue_default)  # May be None for fallback
            self._update_storage_profile_id(farm_id=value, queue_id=new_queue_id)
            self.updating_queue_id_lock = False
        if parameter.name == "queue_id" and not self.updating_queue_id_lock:
            farm_id = self.node.get_parameter_value("farm_id")
            self._update_storage_profile_id(farm_id=farm_id, queue_id=value)

    @classmethod
    def get_param_names(cls) -> list[str]:
        return [
            "priority",
            "initial_state",
            "max_failed_tasks",
            "max_task_retries",
            "farm_id",
            "queue_id",
            "storage_profile_id",
            "conda_channels",
            "conda_packages",
        ]

    def _update_storage_profile_id(self, farm_id: str | None, queue_id: str | None) -> None:
        storage_profile_choices = self._get_storage_profile_choices_values_map(farm_id=farm_id, queue_id=queue_id)
        storage_profile_id_default = self._get_default_choice(
            storage_profile_choices, BaseDeadlineCloud._get_default_storage_profile_id()
        )
        self._update_option_choices(
            param="storage_profile_id",
            choices=list(storage_profile_choices.keys()),
            choices_value_lookup=storage_profile_choices,
            default=storage_profile_id_default,
        )

    def _get_choices_values_map_fallback(self) -> dict:
        """The fallback choices/values map if no farms, queues, or storage profiles are found.

        This is important especially when running in the Published environment, since credentials may not be available.
        """
        return {"None": None}

    def _get_farm_choices_values_map(self) -> dict:
        farms = self.list_farms()
        return (
            {x["displayName"]: x["farmId"] for x in farms}
            if len(farms) > 0
            else self._get_choices_values_map_fallback()
        )

    def _get_queue_choices_values_map(self, farm_id: str | None) -> dict:
        queues = []
        if farm_id is not None and farm_id != "":
            try:
                queues = self.list_queues(farm_id=farm_id)
            except Exception:
                msg = f"Failed to list queues for farm '{farm_id}'"
                logger.exception(msg)
        return (
            {x["displayName"]: x["queueId"] for x in queues}
            if len(queues) > 0
            else self._get_choices_values_map_fallback()
        )

    def _get_storage_profile_choices_values_map(self, farm_id: str | None, queue_id: str | None) -> dict:
        storage_profiles = []
        if farm_id is not None and farm_id != "" and queue_id is not None and queue_id != "":
            try:
                storage_profiles = self.list_storage_profiles(farm_id=farm_id, queue_id=queue_id)
            except Exception:
                msg = f"Failed to list storage profiles for farm '{farm_id}' and queue '{queue_id}'"
                logger.exception(msg)
        return (
            {x["displayName"]: x["storageProfileId"] for x in storage_profiles}
            if len(storage_profiles) > 0
            else self._get_choices_values_map_fallback()
        )

    def _get_queue_id_options(self, farm_id: str | None = None) -> DeadlineCloudResourceOptions:
        if farm_id is None:
            farm_id = self.node.get_parameter_value("farm_id")

        choices: list[str] = []
        choices_value_lookup: dict = {}
        if farm_id is not None and farm_id != "":
            queue_choices_value_map = self._get_queue_choices_values_map(farm_id=farm_id)
            choices = list(queue_choices_value_map.keys())
            choices_value_lookup = queue_choices_value_map
        return DeadlineCloudResourceOptions(
            choices=choices,
            choices_value_lookup=choices_value_lookup,
            value_field="queueId",
        )

    def _get_storage_profile_id_options(
        self, farm_id: str | None = None, queue_id: str | None = None
    ) -> DeadlineCloudResourceOptions:
        if farm_id is None:
            farm_id = self.node.get_parameter_value("farm_id")
        if queue_id is None:
            queue_id = self.node.get_parameter_value("queue_id")

        choices_values_fallback = self._get_choices_values_map_fallback()
        choices: list[str] = ["None"]
        choices_value_lookup: dict = choices_values_fallback
        if farm_id is not None and farm_id != "" and queue_id is not None and queue_id != "":
            storage_profile_choices_value_map = self._get_storage_profile_choices_values_map(
                farm_id=farm_id, queue_id=queue_id
            )
            choices = list(storage_profile_choices_value_map.keys())
            choices_value_lookup = storage_profile_choices_value_map
            if len(choices) == 0:
                choices = ["None"]
                choices_value_lookup = choices_values_fallback
        return DeadlineCloudResourceOptions(
            choices=choices,
            choices_value_lookup=choices_value_lookup,
            value_field="storageProfileId",
        )

    def _update_option_choices(self, param: str, choices: list[str], choices_value_lookup: dict, default: str) -> None:
        parameter = self.node.get_parameter_by_name(param)
        if parameter is not None:
            # Find the DeadlineCloudResourceOptions trait by type since element_id is a UUID
            traits = parameter.find_elements_by_type(DeadlineCloudResourceOptions)
            if traits:
                trait = traits[0]  # Take the first Options trait
                trait.choices = choices
                trait.choices_value_lookup = choices_value_lookup
                # Update the manually set UI options to include the new simple_dropdown
                if hasattr(parameter, "_ui_options") and parameter._ui_options:
                    parameter._ui_options["simple_dropdown"] = choices

                if default in choices:
                    parameter.default_value = default
                    self.node.set_parameter_value(param, default)
                else:
                    msg = f"Default choice '{default}' is not in the provided choices."
                    raise ValueError(msg)

            else:
                msg = f"No DeadlineCloudResourceOptions trait found for parameter '{param}'."
                raise ValueError(msg)
        else:
            msg = f"Parameter '{param}' not found for updating choices."
            raise ValueError(msg)
