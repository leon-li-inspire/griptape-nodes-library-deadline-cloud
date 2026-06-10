from __future__ import annotations

import logging
from configparser import ConfigParser
from typing import TYPE_CHECKING, Any, TypeVar

import boto3
from deadline.client.api import (
    get_boto3_session,
    get_storage_profile_for_queue,
)
from deadline.client.api._session import get_user_and_identity_store_id
from deadline.client.config import get_setting_default
from griptape_nodes.retained_mode.griptape_nodes import (
    GriptapeNodes,
)
from publish import DEADLINE_CLOUD_LIBRARY_CONFIG_KEY

if TYPE_CHECKING:
    from collections.abc import Callable

    from botocore.client import BaseClient
    from deadline.job_attachments.models import StorageProfile

# Suppress botocore logs
logging.getLogger("botocore.tokens").setLevel(logging.ERROR)
logging.getLogger("botocore.credentials").setLevel(logging.ERROR)

T = TypeVar("T")


logger = logging.getLogger("base_deadline_cloud")


class BaseDeadlineCloud:
    def __init__(self, session: boto3.Session | None = None) -> None:
        self._session = session if session is not None else self._get_session()
        self._client: BaseClient | None = None

    @classmethod
    def _get_session(cls) -> boto3.Session:
        """Get the boto3 session."""
        profile_name = cls._get_config_value(
            DEADLINE_CLOUD_LIBRARY_CONFIG_KEY, "profile_name", default=get_setting_default("defaults.aws_profile_name")
        )
        region_name = cls._get_config_value(DEADLINE_CLOUD_LIBRARY_CONFIG_KEY, "region_name", default="us-east-1")
        if profile_name != "" and region_name != "":
            try:
                return boto3.Session(profile_name=profile_name, region_name=region_name)

            except Exception:
                msg = f"Failed to create boto3 session with profile '{profile_name}' and region '{region_name}'."
                logger.debug(msg)

        return get_boto3_session()

    @classmethod
    def _get_config_parser(cls) -> ConfigParser:
        """Get a ConfigParser with application config values."""
        config = ConfigParser()

        # Get the profile name from application config
        profile_name = cls._get_config_value(
            DEADLINE_CLOUD_LIBRARY_CONFIG_KEY,
            "profile_name",
            default=get_setting_default("defaults.aws_profile_name"),
        )

        # Create the sections and settings that Deadline SDK expects
        config.add_section("defaults")
        config.set("defaults", "aws_profile_name", profile_name)

        return config

    @classmethod
    def _get_config_value(cls, service: str, value: str, default: Any | None = None) -> Any:
        """Retrieves a configuration value from the ConfigManager."""
        config_value = GriptapeNodes.ConfigManager().get_config_value(f"{service}.{value}")
        if config_value is None and default is None:
            details = f"Failed to get configuration value '{value}' for service '{service}'."
            logger.error(details)
            raise ValueError(details)
        return config_value if config_value is not None else default

    @classmethod
    def _get_secret(cls, secret: str) -> str:
        """Retrieves a secret value from the SecretsManager."""
        secret_value = GriptapeNodes.SecretsManager().get_secret(secret)
        if not secret_value:
            details = f"Failed to get secret:'{secret}'."
            logger.error(details)
            raise ValueError(details)
        return secret_value

    @classmethod
    def _farm_to_name_and_id(cls, farm: Any) -> str:
        return f"{farm['displayName']} ({farm['farmId']})"

    @classmethod
    def _queue_to_name_and_id(cls, queue: Any) -> str:
        return f"{queue['displayName']} ({queue['queueId']})"

    @classmethod
    def _get_default_farm_id(cls) -> str:
        return BaseDeadlineCloud._get_config_value(
            DEADLINE_CLOUD_LIBRARY_CONFIG_KEY, "farm_id", default=get_setting_default("defaults.farm_id")
        )

    @classmethod
    def _get_default_queue_id(cls) -> str:
        return BaseDeadlineCloud._get_config_value(
            DEADLINE_CLOUD_LIBRARY_CONFIG_KEY, "queue_id", default=get_setting_default("defaults.queue_id")
        )

    @classmethod
    def _get_default_storage_profile_id(cls) -> str:
        return BaseDeadlineCloud._get_config_value(
            DEADLINE_CLOUD_LIBRARY_CONFIG_KEY,
            "storage_profile_id",
            default=get_setting_default("settings.storage_profile_id"),
        )

    def _safe_api_call(self, api_func: Callable, *args, raise_on_error: bool = False, **kwargs) -> Any | None:
        """Common error handling for API calls. Returns None on error unless raise_on_error is True."""
        try:
            return api_func(*args, **kwargs)
        except Exception as e:
            msg = f"API call failed: {e}"
            logger.debug(msg)
            if raise_on_error:
                raise
            return None

    def _get_list_api_kwargs(self) -> dict:
        kwargs = {}
        user_id, _ = get_user_and_identity_store_id(config=self._get_config_parser())
        if user_id:
            kwargs["principalId"] = user_id

        return kwargs

    def list_farms(self, *_args, raise_on_error: bool = False) -> Any:
        """List all farms in Deadline Cloud."""
        kwargs = self._get_list_api_kwargs()
        result = self._safe_api_call(lambda: self._get_client().list_farms(**kwargs), raise_on_error=raise_on_error)
        return result["farms"] if result else []

    def list_queues(self, farm_id: str, *_args, raise_on_error: bool = False) -> Any:
        """List all queues in Deadline Cloud."""
        kwargs = self._get_list_api_kwargs()
        result = self._safe_api_call(
            lambda: self._get_client().list_queues(farmId=farm_id, **kwargs), raise_on_error=raise_on_error
        )
        return result["queues"] if result else []

    def list_storage_profiles(self, farm_id: str, queue_id: str, *_args, raise_on_error: bool = False) -> Any:
        """List all storage profiles for the queue in Deadline Cloud."""
        result = self._safe_api_call(
            lambda: self._get_client().list_storage_profiles_for_queue(farmId=farm_id, queueId=queue_id),
            raise_on_error=raise_on_error,
        )
        return result["storageProfiles"] if result else []

    def search_jobs(self, farm_id: str, queue_id: str, page_size: int = 100, item_offset: int = 0) -> Any:
        result = self._safe_api_call(
            lambda: self._get_client().search_jobs(
                farmId=farm_id,
                queueIds=[queue_id],
                pageSize=page_size,
                itemOffset=item_offset,
            )
        )
        return result["jobs"] if result else []

    def _get_client(self) -> BaseClient:
        """Get cached Deadline Cloud client."""
        if self._client is None:
            try:
                self._client = self._session.client(
                    "deadline", region_name=self._get_config_value(DEADLINE_CLOUD_LIBRARY_CONFIG_KEY, "region_name")
                )
            except Exception as e:
                details = "Failed to create Deadline Cloud client. Please ensure your AWS credentials are configured and refreshed."
                logger.error(details)
                raise RuntimeError(details) from e
        return self._client

    def _get_storage_profile_for_queue(
        self, farm_id: str, queue_id: str, storage_profile_id: Any
    ) -> StorageProfile | None:
        if storage_profile_id and storage_profile_id != "":
            return get_storage_profile_for_queue(farm_id, queue_id, storage_profile_id, self._get_client())
        return None
