# (C) 2025 GoodData Corporation
"""Module for validating custom fields input data.

This module provides the `CustomFieldsDataValidator` class, which is responsible
for validating custom fields input data checking for row level and aggregated
constraints.
"""

from collections import Counter
from typing import Any, Type, TypeVar

from custom_fields.models.aliases import (  # type: ignore[import]
    _DatasetId,
    _RawData,
    _WorkspaceId,
)
from custom_fields.models.custom_data_object import (  # type: ignore[import]
    CustomDataset,
    CustomDatasetDefinition,
    CustomFieldDefinition,
    CustomFieldType,
)
from pydantic import BaseModel


class CustomFieldsDataValidator:
    ModelT = TypeVar("ModelT", bound=BaseModel)

    def validate(
        self,
        raw_dataset_definitions: _RawData,
        raw_field_definitions: _RawData,
    ) -> dict[_WorkspaceId, dict[_DatasetId, CustomDataset]]:
        """Validate dataset and field definitions.

        Validates the dataset definitions and field definitions by using Pydantic
        models to check row level constraints, then aggregates the definitions
        per workspace, while checking for integrity on aggregated level, i.e.,
        uniqueness of combinations of identifieres on workspace level.

        Args:
            raw_dataset_definitions (list[dict[str, str]]): List of raw dataset definitions to validate.
            raw_field_definitions (list[dict[str, str]]): List of raw field definitions to validate.
        Returns:
            dict[WorkspaceId, dict[DatasetId, CustomDataset]]:
                Dictionary of validated dataset definitions per workspace,
                where each dataset contains its custom fields:
                ```python
                {
                    "workspace_id_1": {
                        "dataset_id_1": CustomDataset(...),
                        "dataset_id_2": CustomDataset(...),
                    },
                    ...
                }
                ```
        """

        # First, validate the dataset definitions and aggregate them per workspace.
        validated_data = self._validate_dataset_definitions(raw_dataset_definitions)

        # Then validate the field definitions and connect them to the datasets
        validated_data = self._validate_field_definitions(
            validated_data, raw_field_definitions
        )

        return validated_data

    def _validate_with_pydantic(
        self, raw_data: _RawData, model: Type[ModelT]
    ) -> list[ModelT]:
        """Validate data using provided Pydantic model.

        Validates each dict to check row level constraints.

        Args:
            raw_data (list[dict[str, str]]): List of dictionaries containing raw data.
            model (Type[ModelT]): Pydantic model to validate against.
        Returns:
            list[ModelT]: List of validated model instances.
        """
        return [model(**item) for item in raw_data]

    def _validate_dataset_definitions(
        self,
        raw_dataset_definitions: _RawData,
    ) -> dict[_WorkspaceId, dict[_DatasetId, CustomDataset]]:
        dataset_definitions: list[CustomDatasetDefinition] = (
            self._validate_with_pydantic(
                raw_dataset_definitions, CustomDatasetDefinition
            )
        )
        self._check_dataset_combinations(dataset_definitions)

        validated_definitions: dict[_WorkspaceId, dict[_DatasetId, CustomDataset]] = {}
        for definition in dataset_definitions:
            validated_definitions.setdefault(definition.workspace_id, {})[
                definition.dataset_id
            ] = CustomDataset(definition=definition, custom_fields=[])

        return validated_definitions

    def _check_dataset_combinations(
        self, dataset_definitions: list[CustomDatasetDefinition]
    ) -> None:
        """Check integrity of provided dataset definitions.

        Validation criteria:
            - workspace_id + dataset_id must be unique across all dataset definitions.

        Args:
            dataset_definitions (list[CustomDatasetDefinition]): List of dataset definitions to check.
        Raises:
            ValueError: If there are duplicate dataset definitions based on workspace_id and dataset_id.
        """
        workspace_dataset_combinations = [
            (definition.workspace_id, definition.dataset_id)
            for definition in dataset_definitions
        ]
        if len(workspace_dataset_combinations) != len(
            set(workspace_dataset_combinations)
        ):
            duplicates = self._get_duplicates(workspace_dataset_combinations)
            raise ValueError(
                "Duplicate dataset definitions found in the raw dataset "
                + f"definitions (workspace_id, dataset_id): {duplicates}"
            )

    @staticmethod
    def _get_duplicates(list_to_check: list[Any]) -> list[Any]:
        """Get duplicates from a list.

        Args:
            list_to_check (list[Any]): List of items to check for duplicates.
        Returns:
            list[Any]: List of duplicate items.
        """
        counts = Counter(list_to_check)
        return [item for item, count in counts.items() if count > 1]

    def _check_field_combinations(
        self, field_definitions: list[CustomFieldDefinition]
    ) -> None:
        """Check integrity of provided field definitions.

        Validation criteria (per workspace):
            - unique workspace_id + cf_id combinations (only for attribute and fact cf_type)
            - there is no row with the same dataset_id and cf_id (only for date cf_type)

        Args:
            field_definitions (list[CustomFieldDefinition]): List of field definitions to check.
        Raises:
            ValueError: If there are duplicate field definitions based on workspace_id and cf_id.
        """
        workspace_field_combinations: set[tuple[str, str]] = set()
        dataset_field_combinations: set[tuple[str, str]] = set()

        for field in field_definitions:
            if field.cf_type in [CustomFieldType.ATTRIBUTE, CustomFieldType.FACT]:
                combination = (field.workspace_id, field.cf_id)
                if self._set_contains(workspace_field_combinations, combination):
                    raise ValueError(
                        f"Duplicate custom field found for workspace {field.workspace_id} "
                        + f"with field ID {field.cf_id}"
                    )
                workspace_field_combinations.add(combination)
            elif field.cf_type == CustomFieldType.DATE:
                combination = (field.dataset_id, field.cf_id)
                if self._set_contains(dataset_field_combinations, combination):
                    raise ValueError(
                        f"Duplicate custom field found for dataset {field.dataset_id} "
                        + f"with field ID {field.cf_id}"
                    )
                dataset_field_combinations.add(combination)

    @staticmethod
    def _set_contains(set_to_check: set[Any], item: Any) -> bool:
        """Helper function to check if an item is in a set."""
        return item in set_to_check

    def _validate_field_definitions(
        self,
        validated_definitions: dict[_WorkspaceId, dict[_DatasetId, CustomDataset]],
        raw_field_definitions: _RawData,
    ) -> dict[_WorkspaceId, dict[_DatasetId, CustomDataset]]:
        """Validates custom field definitions amd connects them to the datasets.

        Args:
            validated_definitions (dict[WorkspaceId, dict[DatasetId, CustomDataset]]):
                Dictionary of validated dataset definitions per workspace.
            raw_field_definitions (list[dict[str, str]]): List of raw field definitions to validate.
        Returns:
            dict[WorkspaceId, dict[DatasetId, CustomDataset]]:
                Updated dictionary of validated dataset definitions with custom fields added.
        """
        field_definitions: list[CustomFieldDefinition] = self._validate_with_pydantic(
            raw_field_definitions, CustomFieldDefinition
        )
        self._check_field_combinations(field_definitions)

        for field_definition in field_definitions:
            validated_definitions[field_definition.workspace_id][
                field_definition.dataset_id
            ].custom_fields.append(field_definition)

        return validated_definitions
