# (C) 2025 GoodData Corporation
import argparse
from unittest import mock

import gooddata_sdk as gd_sdk
from gooddata_api_client.exceptions import NotFoundException

from scripts import permission_mgmt

TEST_CSV_PATH = "tests/data/permission_mgmt/input.csv"

USER_1 = gd_sdk.CatalogAssigneeIdentifier(id="user_1", type="user")
USER_2 = gd_sdk.CatalogAssigneeIdentifier(id="user_2", type="user")
USER_3 = gd_sdk.CatalogAssigneeIdentifier(id="user_3", type="user")
UG_1 = gd_sdk.CatalogAssigneeIdentifier(id="ug_1", type="userGroup")
UG_2 = gd_sdk.CatalogAssigneeIdentifier(id="ug_2", type="userGroup")
UG_3 = gd_sdk.CatalogAssigneeIdentifier(id="ug_3", type="userGroup")

UPSTREAM_PERMISSIONS = [
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="ANALYZE", assignee=USER_1),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="VIEW", assignee=USER_1),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="MANAGE", assignee=USER_1),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="ANALYZE", assignee=USER_2),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="VIEW", assignee=USER_2),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="ANALYZE", assignee=USER_3),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="ANALYZE", assignee=UG_1),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="VIEW", assignee=UG_1),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="MANAGE", assignee=UG_1),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="ANALYZE", assignee=UG_2),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="VIEW", assignee=UG_2),
    gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="ANALYZE", assignee=UG_3),
]

WS_PERMISSION_DECLARATION = permission_mgmt.WSPermissionDeclaration(
    users={
        "user_1": {"ANALYZE": True, "VIEW": True, "MANAGE": True},
        "user_2": {"ANALYZE": True, "VIEW": True},
        "user_3": {"ANALYZE": True},
    },
    user_groups={
        "ug_1": {"ANALYZE": True, "VIEW": True, "MANAGE": True},
        "ug_2": {"ANALYZE": True, "VIEW": True},
        "ug_3": {"ANALYZE": True},
    },
)

UPSTREAM_WS_PERMISSION = gd_sdk.CatalogDeclarativeWorkspacePermissions(
    permissions=UPSTREAM_PERMISSIONS
)

UPSTREAM_WS_PERMISSIONS = {
    "ws_id_1": UPSTREAM_WS_PERMISSION,
    "ws_id_2": UPSTREAM_WS_PERMISSION,
}

EXPECTED_WS1_PERMISSIONS = gd_sdk.CatalogDeclarativeWorkspacePermissions(
    permissions=[
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="ANALYZE", assignee=USER_1
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="VIEW", assignee=USER_1
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="ANALYZE", assignee=USER_2
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="MANAGE", assignee=USER_2
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="ANALYZE", assignee=USER_3
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="ANALYZE", assignee=UG_1
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(name="VIEW", assignee=UG_1),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="ANALYZE", assignee=UG_2
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="MANAGE", assignee=UG_2
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="ANALYZE", assignee=UG_3
        ),
    ]
)

EXPECTED_WS2_PERMISSIONS = gd_sdk.CatalogDeclarativeWorkspacePermissions(
    permissions=[
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="MANAGE", assignee=USER_1
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="MANAGE", assignee=USER_3
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="MANAGE", assignee=UG_1
        ),
        gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
            name="MANAGE", assignee=UG_3
        ),
    ]
)


def test_declaration_from_populated_sdk_api_obj():
    declaration = permission_mgmt.WSPermissionDeclaration.from_sdk_api(
        UPSTREAM_WS_PERMISSION
    )
    assert declaration == WS_PERMISSION_DECLARATION


def test_declaration_from_empty_sdk_api_obj():
    api_obj = gd_sdk.CatalogDeclarativeWorkspacePermissions(permissions=[])
    declaration = permission_mgmt.WSPermissionDeclaration.from_sdk_api(api_obj)
    assert len(declaration.users) == 0
    assert len(declaration.user_groups) == 0


def test_declaration_to_populated_sdk_api_obj():
    api_obj = permission_mgmt.WSPermissionDeclaration.to_sdk_api(
        WS_PERMISSION_DECLARATION
    )
    assert api_obj == UPSTREAM_WS_PERMISSION


def test_declaration_with_inactive_to_sdk_api_obj():
    users = {
        "user_1": {"ANALYZE": True, "VIEW": False},
        "user_2": {"ANALYZE": True},
    }
    ugs = {
        "ug_1": {"ANALYZE": True, "VIEW": False},
        "ug_2": {"ANALYZE": True},
    }
    declaration = permission_mgmt.WSPermissionDeclaration(users, ugs)
    api_obj = declaration.to_sdk_api()
    expected = gd_sdk.CatalogDeclarativeWorkspacePermissions(
        permissions=[
            gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
                name="ANALYZE", assignee=USER_1
            ),
            gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
                name="ANALYZE", assignee=USER_2
            ),
            gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
                name="ANALYZE", assignee=UG_1
            ),
            gd_sdk.CatalogDeclarativeSingleWorkspacePermission(
                name="ANALYZE", assignee=UG_2
            ),
        ]
    )
    assert api_obj == expected


def test_declaration_with_only_inactive_to_sdk_api_obj():
    users = {
        "user_1": {"ANALYZE": False, "VIEW": False},
        "user_2": {"ANALYZE": False},
    }
    ugs = {
        "ug_1": {"ANALYZE": False, "VIEW": False},
        "ug_2": {"ANALYZE": False},
    }
    declaration = permission_mgmt.WSPermissionDeclaration(users, ugs)
    api_obj = declaration.to_sdk_api()
    expected = gd_sdk.CatalogDeclarativeWorkspacePermissions(permissions=[])
    assert api_obj == expected


# Declarations are explicitly defined anew here to avoid dict mutations
# in subsequent calls and to avoid dict deepcopy overhead.


def test_add_new_active_user_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("MANAGE", "", "user_1", "user", True)
    declaration.add_permission(permission)
    assert declaration.users == {
        "user_1": {"ANALYZE": True, "VIEW": False, "MANAGE": True}
    }
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


def test_add_new_inactive_user_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("MANAGE", "", "user_1", "user", False)
    declaration.add_permission(permission)
    assert declaration.users == {
        "user_1": {"ANALYZE": True, "VIEW": False, "MANAGE": False}
    }
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


def test_overwrite_inactive_user_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("VIEW", "", "user_1", "user", True)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": True}}
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


def test_overwrite_active_user_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("ANALYZE", "", "user_1", "user", False)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


def test_add_new_user_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("VIEW", "", "user_2", "user", True)
    declaration.add_permission(permission)
    assert declaration.users == {
        "user_1": {"ANALYZE": True, "VIEW": False},
        "user_2": {"VIEW": True},
    }
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


def test_modify_one_of_user_perms():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}, "user_2": {"VIEW": True}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("MANAGE", "", "user_1", "user", True)
    declaration.add_permission(permission)
    assert declaration.users == {
        "user_1": {"ANALYZE": True, "VIEW": False, "MANAGE": True},
        "user_2": {"VIEW": True},
    }
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


# Add userGroup permission


def test_add_new_active_ug_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("MANAGE", "", "ug_1", "userGroup", True)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {
        "ug_1": {"VIEW": True, "ANALYZE": False, "MANAGE": True}
    }


def test_add_new_inactive_ug_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("MANAGE", "", "ug_1", "userGroup", False)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {
        "ug_1": {"VIEW": True, "ANALYZE": False, "MANAGE": False}
    }


def test_overwrite_inactive_ug_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("ANALYZE", "", "ug_1", "userGroup", True)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": True}}


def test_overwrite_active_ug_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("VIEW", "", "ug_1", "userGroup", False)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {"ug_1": {"VIEW": True, "ANALYZE": False}}


def test_add_new_ug_perm():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}},
    )
    permission = permission_mgmt.WSPermission("VIEW", "", "ug_2", "userGroup", True)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {
        "ug_1": {"VIEW": True, "ANALYZE": False},
        "ug_2": {"VIEW": True},
    }


def test_modify_one_of_ug_perms():
    declaration = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True, "VIEW": False}},
        {"ug_1": {"VIEW": True, "ANALYZE": False}, "ug_2": {"VIEW": True}},
    )
    permission = permission_mgmt.WSPermission("MANAGE", "", "ug_1", "userGroup", True)
    declaration.add_permission(permission)
    assert declaration.users == {"user_1": {"ANALYZE": True, "VIEW": False}}
    assert declaration.user_groups == {
        "ug_1": {"VIEW": True, "ANALYZE": False, "MANAGE": True},
        "ug_2": {"VIEW": True},
    }


def test_upsert():
    owner = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"ANALYZE": True}, "user_2": {"VIEW": True}},
        {"ug_1": {"ANALYZE": True}, "ug_2": {"VIEW": True}},
    )
    other = permission_mgmt.WSPermissionDeclaration(
        {"user_1": {"MANAGE": True, "VIEW": False}},
        {"ug_2": {"MANAGE": True, "VIEW": False}},
    )
    owner.upsert(other)
    assert owner.users == {
        "user_1": {"MANAGE": True, "VIEW": False},
        "user_2": {"VIEW": True},
    }
    assert owner.user_groups == {
        "ug_1": {"ANALYZE": True},
        "ug_2": {"MANAGE": True, "VIEW": False},
    }


def mock_upstream_perms(ws_id: str) -> gd_sdk.CatalogDeclarativeWorkspacePermissions:
    if ws_id not in UPSTREAM_WS_PERMISSIONS:
        raise NotFoundException(404)
    return UPSTREAM_WS_PERMISSIONS[ws_id]


@mock.patch("scripts.permission_mgmt.create_client")
def test_permission_management_e2e(create_client):
    sdk = mock.Mock()
    sdk.catalog_permission.get_declarative_permissions.side_effect = mock_upstream_perms
    create_client.return_value = sdk

    args = argparse.Namespace(perm_csv=TEST_CSV_PATH, verbose=False)

    permission_mgmt.permission_mgmt(args)

    sdk.catalog_permission.put_declarative_permissions.assert_has_calls(
        [
            mock.call("ws_id_1", EXPECTED_WS1_PERMISSIONS),
            mock.call("ws_id_2", EXPECTED_WS2_PERMISSIONS),
        ]
    )
