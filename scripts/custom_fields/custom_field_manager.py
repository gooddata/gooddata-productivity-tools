# (C) 2025 GoodData Corporation
"""Module orchestrating the custom fields logic."""

from custom_fields.api import GoodDataAPI  # type: ignore[import]
from custom_fields.input_processor import (  # type: ignore[import]
    CustomFieldsDataProcessor,
)
from custom_fields.input_validator import (  # type: ignore[import]
    CustomFieldsDataValidator,
)
from custom_fields.models.aliases import (  # type: ignore[import]
    _DatasetId,
    _WorkspaceId,
)
from custom_fields.models.analytical_object import (  # type: ignore[import]
    AnalyticalObject,
    AnalyticalObjects,
)
from custom_fields.models.custom_data_object import (  # type: ignore[import]
    CustomDataset,
)
from gooddata_sdk.sdk import GoodDataSdk


class CustomFieldManager:
    """Manager for creating custom datasets and fields in GoodData workspaces."""

    INDENT = " " * 2

    def __init__(self, host: str, token: str):
        self._validator = CustomFieldsDataValidator()
        self._processor = CustomFieldsDataProcessor()
        self._sdk = GoodDataSdk.create(host_=host, token_=token)
        self._api = GoodDataAPI(host=host, token=token)

    def _get_objects_with_invalid_relations(
        self, workspace_id: str
    ) -> list[AnalyticalObject]:
        """Check for invalid references in the provided analytical objects.

        This method checks if the references in the provided analytical objects
        are valid. It returns a set of analytical objects that have invalid references.

        Args:
            workspace_id (str): The ID of the workspace to check.

        Returns:
            list[AnalyticalObject]: Set of analytical objects with invalid references.
        """

        analytical_objects: list[AnalyticalObject] = self._get_analytical_objects(
            workspace_id=workspace_id
        )

        objects_with_invalid_relations = [
            obj for obj in analytical_objects if not obj.attributes.are_relations_valid
        ]
        return objects_with_invalid_relations

    def _get_analytical_objects(self, workspace_id: str) -> list[AnalyticalObject]:
        """Get analytical objects in the workspace.

        This method retrieves all analytical objects (metrics, visualizations, dashboards)
        in the specified workspace and returns them as a list.

        Args:
            workspace_id (str): The ID of the workspace to retrieve objects from.

        Returns:
            list[AnalyticalObject]: List of analytical objects in the workspace.
        """
        metrics_response = self._api.get_all_metrics(workspace_id)
        visualizations_response = self._api.get_all_visualization_objects(workspace_id)
        dashboards_response = self._api.get_all_dashboards(workspace_id)
        self._api.raise_if_response_not_ok(
            metrics_response,
            visualizations_response,
            dashboards_response,
        )
        metrics = AnalyticalObjects(**metrics_response.json())
        visualizations = AnalyticalObjects(**visualizations_response.json())
        dashboards = AnalyticalObjects(**dashboards_response.json())

        return metrics.data + visualizations.data + dashboards.data

    def _new_ldm_does_not_invalidate_relations(
        self,
        current_invalid_relations: list[AnalyticalObject],
        new_invalid_relations: list[AnalyticalObject],
    ) -> bool:
        # Create a set of IDs for each group, then compare those sets
        set_current_invalid_relations = {obj.id for obj in current_invalid_relations}
        set_new_invalid_relations = {obj.id for obj in new_invalid_relations}
        if set_current_invalid_relations == set_new_invalid_relations:
            # No new invalid references found, success
            return True
        elif len(set_new_invalid_relations) < len(set_current_invalid_relations):
            # Fewer invalid references found, success
            return True
        else:
            # More invalid references found, failure
            return False

    def _process_with_relations_check(
        self, validated_data: dict[_WorkspaceId, dict[_DatasetId, CustomDataset]]
    ) -> None:
        """Check whether relations of analytical objects are valid before and after
        updating the LDM in the GoodData workspace.
        """
        # Iterate through the workspaces.
        for workspace_id, datasets in validated_data.items():
            # Get current workspace layout
            current_layout = self._api.get_workspace_layout(workspace_id)
            # Get a set of objects with invalid relations from current workspace state
            current_invalid_relations = self._get_objects_with_invalid_relations(
                workspace_id=workspace_id
            )

            # Put the LDM with custom datasets into the GoodData workspace.
            self._sdk.catalog_workspace_content.put_declarative_ldm(
                workspace_id=workspace_id,
                ldm=self._processor.datasets_to_ldm(datasets),
            )

            # Get a set of objects with invalid relations from the new workspace state
            new_invalid_relations = self._get_objects_with_invalid_relations(
                workspace_id=workspace_id
            )

            if self._new_ldm_does_not_invalidate_relations(
                current_invalid_relations, new_invalid_relations
            ):
                self._print_success_message(workspace_id)
                return

            print(
                f"❌ Difference in invalid relations found in workspace {workspace_id}."
            )
            self._print_diff_invalid_relations(
                current_invalid_relations, new_invalid_relations
            )

            print(
                f"{self.INDENT}⚠️ Reverting the workspace layout to the original state."
            )
            # Put the original workspace layout back to the workspace
            revert_response = self._api.put_workspace_layout(
                workspace_id=workspace_id, layout=current_layout.json()
            )

            if not revert_response.ok:
                print(f"Failed to revert workspace layout in {workspace_id}.")
                print(f"Error: {revert_response.status_code} - {revert_response.text}")

    def _print_diff_invalid_relations(
        self,
        current_invalid_relations: list[AnalyticalObject],
        new_invalid_relations: list[AnalyticalObject],
    ) -> None:
        """Prints out objects with newly invalid relations.

        Objects which previously did not have invalid relations, but do so after
        updating the LDM, are printed out.
        """
        print(f"{self.INDENT}Objects with newly invalidated relations:")
        for obj in new_invalid_relations:
            if obj not in current_invalid_relations:
                print(f"{self.INDENT}∙ {obj.id} ({obj.type}) {obj.attributes.title}")

    def _process_without_relations_check(
        self, validated_data: dict[_WorkspaceId, dict[_DatasetId, CustomDataset]]
    ) -> None:
        """Update the LDM in the GoodData workspace without checking relations."""
        for workspace_id, datasets in validated_data.items():
            # Put the LDM with custom datasets into the GoodData workspace.
            self._sdk.catalog_workspace_content.put_declarative_ldm(
                workspace_id=workspace_id,
                ldm=self._processor.datasets_to_ldm(datasets),
            )
            self._print_success_message(workspace_id)

    def _print_success_message(self, workspace_id: str) -> None:
        """Print a success message after updating the workspace LDM."""
        print(f"✅ LDM in {workspace_id} updated successfully.")

    def process(
        self,
        raw_custom_datasets: list[dict[str, str]],
        raw_custom_fields: list[dict[str, str]],
        check_relations: bool,
    ) -> None:
        """Create custom datasets and fields in GoodData workspaces.

        Creates custom datasets and fields to extend the Logical Data Model (LDM)
        in GoodData workspaces based on the provided raw data definitions. The raw
        data is validated by Pydantic models (CustomDatasetDefinition and CustomFieldDefinition).
        The defined datasets and fields are then uploaded to GoodData Cloud.

        Args:
            raw_custom_datasets (list[dict[str, str]]): List of raw custom dataset definitions.
            raw_custom_fields (list[dict[str, str]]): List of raw custom field definitions.
            check_relations (bool): If True, checks for invalid relations in the workspace
                after updating the LDM. If the number of invalid relations increases,
                the LDM will be reverted to its previous state. If False, the check
                is skiped and the LDM is updated directly. Defaults to True.

        Raises:
            ValueError: If there are validation errors in the dataset or field definitions.
        """
        # Validate raw data and aggregate the custom field and dataset
        # definitions per workspace.
        validated_data: dict[_WorkspaceId, dict[_DatasetId, CustomDataset]] = (
            self._validator.validate(raw_custom_datasets, raw_custom_fields)
        )

        if check_relations:
            # Process the validated data with relations check.
            self._process_with_relations_check(validated_data)
        else:
            self._process_without_relations_check(validated_data)
