import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../scripts"))
)
import pytest
from custom_fields.custom_field_manager import CustomFieldManager
from pytest_mock import MockerFixture


@pytest.fixture
def manager(mocker: MockerFixture):
    # Patch dependencies in the constructor
    mocker.patch("scripts.custom_fields.custom_field_manager.CustomFieldsDataValidator")
    mocker.patch("scripts.custom_fields.custom_field_manager.CustomFieldsDataProcessor")
    mocker.patch("scripts.custom_fields.custom_field_manager.GoodDataSdk")
    mocker.patch("scripts.custom_fields.custom_field_manager.GoodDataAPI")
    return CustomFieldManager(host="host", token="token")


@pytest.fixture
def validated_data(mocker: MockerFixture):
    # Minimal valid structure for validated_data
    return {"workspace_1": {"dataset_1": mocker.MagicMock()}}


def make_analytical_object(mocker: MockerFixture, id, title="Title", type="type"):
    obj = mocker.MagicMock()
    obj.id = id
    obj.type = type
    obj.attributes.title = title
    return obj


def test_relations_check_success(manager, validated_data, mocker: MockerFixture):
    """Relation check passes, workspace layout not reverted."""
    # Setup mocks
    mocker.patch.object(
        manager._api,
        "get_workspace_layout",
        return_value=mocker.MagicMock(
            json=mocker.MagicMock(return_value="layout_json")
        ),
    )
    mocker.patch.object(
        manager,
        "_get_analytical_objects",
        side_effect=[
            [make_analytical_object(mocker, "a", "A")],  # current
            [make_analytical_object(mocker, "a", "A")],  # new
        ],
    )
    mocker.patch.object(
        manager,
        "_get_objects_with_invalid_relations",
        side_effect=[
            set(),  # current_invalid_relations
            set(),  # new_invalid_relations
        ],
    )
    mocker.patch.object(manager._processor, "datasets_to_ldm", return_value="ldm")
    mocker.patch.object(manager._sdk.catalog_workspace_content, "put_declarative_ldm")
    mocker.patch.object(
        manager, "_new_ldm_does_not_invalidate_relations", return_value=True
    )
    mocker.patch.object(manager._api, "put_workspace_layout")

    # Should print "Workspace workspace_1 LDM updated." and not revert
    manager._process_with_relations_check(validated_data)
    manager._sdk.catalog_workspace_content.put_declarative_ldm.assert_called_once()
    manager._api.put_workspace_layout.assert_not_called()


def test_relations_check_failure_and_revert(
    manager, validated_data, capsys, mocker: MockerFixture
):
    """Relation check fails, workspace layout is reverted."""
    # Setup mocks
    mocker.patch.object(
        manager._api,
        "get_workspace_layout",
        return_value=mocker.MagicMock(
            json=mocker.MagicMock(return_value="layout_json")
        ),
    )
    obj1 = make_analytical_object(mocker, "a", "A")
    obj2 = make_analytical_object(mocker, "b", "B")
    mocker.patch.object(
        manager,
        "_get_objects_with_invalid_relations",
        side_effect=[
            {obj1},  # current_invalid_relations
            {obj1, obj2},  # new_invalid_relations (one more invalid)
        ],
    )
    mocker.patch.object(manager._processor, "datasets_to_ldm", return_value="ldm")
    mocker.patch.object(manager._sdk.catalog_workspace_content, "put_declarative_ldm")
    mocker.patch.object(
        manager, "_new_ldm_does_not_invalidate_relations", return_value=False
    )
    mocker.patch.object(manager._api, "put_workspace_layout")

    manager._process_with_relations_check(validated_data)

    # Should revert and print info about invalid relations
    manager._api.put_workspace_layout.assert_called_once_with(
        workspace_id="workspace_1", layout="layout_json"
    )
    out = capsys.readouterr().out
    assert "Difference in invalid relations found in workspace workspace_1." in out
    assert "b (type) B" in out
    assert "Reverting the workspace layout to the original state." in out


def test_relations_check_fewer_invalid_relations(
    manager, validated_data, mocker: MockerFixture
):
    """Fewer invalid relations after LDM update, no revert needed."""
    # Setup mocks
    obj1 = make_analytical_object(mocker, "a", "A")
    mocker.patch.object(
        manager._api,
        "get_workspace_layout",
        return_value=mocker.MagicMock(
            json=mocker.MagicMock(return_value="layout_json")
        ),
    )
    mocker.patch.object(
        manager,
        "_get_objects_with_invalid_relations",
        side_effect=[
            {
                obj1,
                make_analytical_object(mocker, "b", "B"),
            },  # current_invalid_relations
            {obj1},  # new_invalid_relations (fewer)
        ],
    )
    mocker.patch.object(manager._processor, "datasets_to_ldm", return_value="ldm")
    mocker.patch.object(manager._sdk.catalog_workspace_content, "put_declarative_ldm")
    mocker.patch.object(
        manager, "_new_ldm_does_not_invalidate_relations", return_value=True
    )
    mocker.patch.object(manager._api, "put_workspace_layout")

    manager._process_with_relations_check(validated_data)
    manager._api.put_workspace_layout.assert_not_called()
