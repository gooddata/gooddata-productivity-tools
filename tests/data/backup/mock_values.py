from gooddata_sdk.catalog.identifier import CatalogWorkspaceIdentifier
from gooddata_sdk.catalog.workspace.declarative_model.workspace.workspace import (
    CatalogDeclarativeWorkspace,
    CatalogDeclarativeWorkspaces,
)

NO_CHILDREN_RETURN_VALUE = CatalogDeclarativeWorkspaces(
    workspaces=[
        CatalogDeclarativeWorkspace(
            id="ws_id",
            name="ws_name",
            parent=CatalogWorkspaceIdentifier(id="recognized_parent_id"),
        )
    ],
    workspace_data_filters=[],
)

WORKSPACE_HIERARCHY = CatalogDeclarativeWorkspaces(
    workspaces=[
        CatalogDeclarativeWorkspace(
            id="parent",
            name="parent",
            parent=None,
        ),
        CatalogDeclarativeWorkspace(
            id="direct_child",
            name="direct_child",
            parent=CatalogWorkspaceIdentifier(id="parent"),
        ),
        CatalogDeclarativeWorkspace(
            id="indirect_child",
            name="indirect_child",
            parent=CatalogWorkspaceIdentifier(id="direct_child"),
        ),
        CatalogDeclarativeWorkspace(
            id="another_direct_child",
            name="another_direct_child",
            parent=CatalogWorkspaceIdentifier(id="parent"),
        ),
        CatalogDeclarativeWorkspace(
            id="another_indirect_child",
            name="another_indirect_child",
            parent=CatalogWorkspaceIdentifier(id="another_direct_child"),
        ),
        CatalogDeclarativeWorkspace(
            id="unrelated_workspace",
            name="unrelated_workspace",
            parent=None,
        ),
        CatalogDeclarativeWorkspace(
            id="another_unrelated_workspace",
            name="another_unrelated_workspace",
            parent=None,
        ),
    ],
    workspace_data_filters=[],
)
