# (C) 2025 GoodData Corporation
import argparse
from dataclasses import dataclass
from typing import Any, Optional
from unittest import mock

import gooddata_sdk as gd_sdk
import pytest
from gooddata_api_client.exceptions import NotFoundException

from scripts import user_mgmt

TEST_CSV_PATH = "tests/data/user_mgmt/input.csv"


@dataclass
class MockUser:
    id: str
    firstname: Optional[str]
    lastname: Optional[str]
    email: Optional[str]
    authenticationId: Optional[str]
    user_groups: list[str]

    def to_sdk(self):
        return gd_sdk.CatalogUser.init(
            user_id=self.id,
            firstname=self.firstname,
            lastname=self.lastname,
            email=self.email,
            authentication_id=self.authenticationId,
            user_group_ids=self.user_groups,
        )

    def to_json(self):
        attrs = {}
        if self.authenticationId:
            attrs["authenticationId"] = self.authenticationId
        if self.firstname:
            attrs["firstname"] = self.firstname
        if self.lastname:
            attrs["lastname"] = self.lastname
        if self.email:
            attrs["email"] = self.email

        data = {
            "id": self.id,
            "type": "user",
            "attributes": attrs,
        }

        if not self.user_groups:
            return data

        relsdata = [{"id": group, "type": "userGroup"} for group in self.user_groups]
        if relsdata:
            data["relationships"] = {"userGroups": {"data": relsdata}}
        return data


@mock.patch("os.path.exists")
def test_conflicting_delimiters_raises_error(path_exists):
    path_exists.return_value = True
    args = argparse.Namespace(
        conf="", user_csv="", delimiter=",", ug_delimiter=",", quotechar='"'
    )
    with pytest.raises(RuntimeError):
        user_mgmt.validate_args(args)


def test_user_obj_from_sdk():
    user_input = MockUser("some.user", "some", "user", "some@email.com", "auth", ["ug"])
    excepted = user_mgmt.GDUserTarget(
        "some.user", "some", "user", "some@email.com", "auth", ["ug"], True
    )
    user = user_mgmt.GDUserTarget.from_sdk_obj(user_input.to_sdk())
    assert excepted == user


def test_user_obj_from_sdk_no_ugs():
    user_input = MockUser("some.user", "some", "user", "some@email.com", "auth", [])
    excepted = user_mgmt.GDUserTarget(
        "some.user", "some", "user", "some@email.com", "auth", [], True
    )
    user = user_mgmt.GDUserTarget.from_sdk_obj(user_input.to_sdk())
    assert excepted == user


def test_user_obj_to_sdk():
    user_input = MockUser("some.user", "some", "user", "some@email.com", "auth", ["ug"])
    user = user_mgmt.GDUserTarget(
        "some.user", "some", "user", "some@email.com", "auth", ["ug"], True
    )
    excepted = user_input.to_sdk()
    assert excepted == user.to_sdk_obj()


def test_user_obj_to_sdk_no_ugs():
    user_input = MockUser("some.user", "some", "user", "some@email.com", "auth", [])
    user = user_mgmt.GDUserTarget(
        "some.user", "some", "user", "some@email.com", "auth", [], True
    )
    excepted = user_input.to_sdk()
    assert excepted == user.to_sdk_obj()


class MockResponse:
    def __init__(self, status_code, json_response: dict[str, Any] = {}, text: str = ""):
        self.status_code = status_code
        self.json_response = json_response
        self.text = text

    def json(self):
        return self.json_response


UPSTREAM_USERS = {
    "jozef.mrkva": MockUser(
        "jozef.mrkva", "jozef", "mrkva", "jozef.mrkva@test.com", "auth_id_1", []
    ),
    "kristian.kalerab": MockUser(
        "kristian.kalerab",
        "kristian",
        "kalerab",
        "kristian.kalerab@test.com",
        "auth_id_5",
        [],
    ),
    "richard.cvikla": MockUser(
        "richard.cvikla", "richard", "cvikla", None, "auth_id_6", []
    ),
    "adam.avokado": MockUser("adam.avokado", None, None, None, "auth_id_7", []),
}

UPSTREAM_UG_ID = "ug_1"
EXPECTED_NEW_UG_OBJ = gd_sdk.CatalogUserGroup.init("ug_2", "ug_2")
EXPECTED_GET_IDS = {"jozef.mrkva", "kristian.kalerab", "peter.pertzlen", "zoltan.zeler"}
EXPECTED_CREATE_OR_UPDATE_IDS = {"peter.pertzlen", "zoltan.zeler", "kristian.kalerab"}


def prepare_sdk():
    def mock_get_user(user_id):
        if user_id not in UPSTREAM_USERS:
            raise NotFoundException
        return UPSTREAM_USERS[user_id].to_sdk()

    def mock_get_user_group(ug_id):
        if ug_id != UPSTREAM_UG_ID:
            raise NotFoundException
        return

    sdk = mock.Mock()
    sdk.catalog_user.get_user.side_effect = mock_get_user
    sdk.catalog_user.get_user_group.side_effect = mock_get_user_group
    return sdk


"""
jozef - No change; user exists
bartolomej - no change; user doesnt exist
peter - create (2 ugs); 1 ug exists, 1 doesnt
zoltan - create (1 ug); ug exists
kristian - update
richard - delete (diff fields than in upstream)
adam - delete (same fields as in upstream)
"""


@mock.patch("scripts.user_mgmt.create_clients")
def test_user_mgmt_e2e(create_client):
    sdk = prepare_sdk()
    create_client.return_value = sdk

    args = argparse.Namespace(
        user_csv=TEST_CSV_PATH,
        delimiter=",",
        ug_delimiter="|",
        quotechar='"',
        verbose=False,
    )

    user_mgmt.user_mgmt(args)

    sdk.catalog_user.get_user.assert_has_calls(
        [mock.call(id) for id in EXPECTED_GET_IDS],
        any_order=True,
    )

    created_or_updated = {
        call[0][0].id for call in sdk.catalog_user.create_or_update_user.call_args_list
    }
    assert created_or_updated == EXPECTED_CREATE_OR_UPDATE_IDS

    sdk.catalog_user.delete_user.assert_has_calls(
        [mock.call("richard.cvikla"), mock.call("adam.avokado")]
    )
    sdk.catalog_user.get_user_group.assert_has_calls(
        [mock.call("ug_1"), mock.call("ug_2")]
    )
    sdk.catalog_user.create_or_update_user_group.assert_called_once_with(
        EXPECTED_NEW_UG_OBJ
    )
