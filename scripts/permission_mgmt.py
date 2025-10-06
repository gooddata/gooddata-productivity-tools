# (C) 2025 GoodData Corporation
import argparse
import os
from pathlib import Path

from gooddata_pipelines import (
    EntityType,
    PermissionIncrementalLoad,
    PermissionProvisioner,
)
from gooddata_sdk.utils import PROFILES_FILE_PATH
from utils.logger import get_logger, setup_logging  # type: ignore[import]
from utils.utils import (  # type: ignore[import]
    create_provisioner,
    read_csv_file_to_dict,
)

setup_logging()
logger = get_logger(__name__)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Management of workspace permissions.")
    parser.add_argument(
        "perm_csv",
        type=Path,
        help=(
            "Path to (comma-delimited) csv with user/userGroup "
            "to workspace permission pairs."
        ),
    )
    parser.add_argument(
        "-d",
        "--delimiter",
        type=str,
        default=",",
        help="Delimiter used to separate different columns in the user_csv.",
    )
    parser.add_argument(
        "-p",
        "--profile-config",
        type=Path,
        default=PROFILES_FILE_PATH,
        help="Optional path to GoodData profile config. "
        f'If no path is provided, "{PROFILES_FILE_PATH}" is used.',
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="default",
        help='GoodData profile to use. If not profile is provided, "default" is used.',
    )
    return parser


def read_permissions_from_csv(
    args: argparse.Namespace,
) -> list[PermissionIncrementalLoad]:
    """Reads permissions from the input csv file."""
    validated_permissions: list[PermissionIncrementalLoad] = []
    raw_permissions = read_csv_file_to_dict(args.perm_csv, args.delimiter)

    for raw_permission in raw_permissions:
        try:
            if raw_permission["user_id"] and raw_permission["ug_id"]:
                raise RuntimeError(
                    "UserID and UserGroupID are mutually exclusive per csv row. "
                    f'Skipping following row: "{raw_permission}".'
                )

            entity_id = raw_permission["user_id"] or raw_permission["ug_id"]
            if not entity_id:
                raise RuntimeError(
                    "Either UserID or UserGroupID have to be defined per csv row. "
                    f'Skipping following row: "{raw_permission}".'
                )

            if raw_permission["user_id"]:
                entity_type = EntityType.user
            else:
                entity_type = EntityType.user_group

            validated_permission = PermissionIncrementalLoad(
                permission=raw_permission["ws_permissions"],
                workspace_id=raw_permission["ws_id"],
                entity_id=entity_id,
                entity_type=entity_type,
                is_active=raw_permission["is_active"],
            )
            validated_permissions.append(validated_permission)
        except KeyError as e:
            logger.error(f"Missing key in following row: {raw_permission}. Error: {e}")
            continue
        except Exception as e:
            logger.error(
                f'Unable to load following row: "{raw_permission}". Error: "{e}"'
            )
            continue

    return validated_permissions


def validate_args(args: argparse.Namespace) -> None:
    """Validates the input arguments."""
    if not os.path.exists(args.perm_csv):
        raise RuntimeError(
            "Invalid path to workspace permission management input csv given."
        )


def permission_mgmt():
    parser = create_parser()
    args = parser.parse_args()

    permissions = read_permissions_from_csv(args)

    permission_manager = create_provisioner(
        PermissionProvisioner, args.profile_config, args.profile
    )

    permission_manager.logger.subscribe(logger)

    permission_manager.incremental_load(permissions)


if __name__ == "__main__":
    permission_mgmt()
