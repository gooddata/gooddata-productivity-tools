# BSD License
#
# Copyright (c) 2024, GoodData Corporation. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted, provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import argparse
import pytest
from unittest import mock
from dataclasses import dataclass

from gooddata_sdk.catalog.user.entity_model.user import CatalogUserGroup
from scripts import user_group_mgmt

TEST_CSV_PATH = "tests/data/user_group_mgmt/input.csv"


@dataclass
class MockUserGroup:
    id: str
    name: str
    parent_ids: list[str]

    def to_sdk(self):
        return CatalogUserGroup.init(
            user_group_id=self.id,
            user_group_name=self.name,
            user_group_parent_ids=self.parent_ids,
        )


@mock.patch("os.path.exists")
def test_conflicting_delimiters_raises_error(path_exists):
    path_exists.return_value = True
    args = argparse.Namespace(
        user_group_csv="", delimiter=",", ug_delimiter=",", quotechar='"'
    )
    with pytest.raises(RuntimeError):
        user_group_mgmt.validate_args(args)


def test_from_csv_row_standard():
    row = ["ug_1", "Admins", "ug_2|ug_3", "True"]
    result = user_group_mgmt.TargetUserGroup.from_csv_row(row, "|")
    expected = user_group_mgmt.TargetUserGroup(
        user_group_id="ug_1",
        user_group_name="Admins",
        parent_user_groups=["ug_2", "ug_3"],
        is_active=True,
    )
    assert result == expected, "Standard row should be parsed correctly"


def test_from_csv_row_no_parent_groups():
    row = ["ug_2", "Developers", "", "True"]
    result = user_group_mgmt.TargetUserGroup.from_csv_row(row, "|")
    expected = user_group_mgmt.TargetUserGroup(
        user_group_id="ug_2",
        user_group_name="Developers",
        parent_user_groups=[],
        is_active=True,
    )
    assert (
        result == expected
    ), "Row without parent user groups should be parsed correctly"


def test_from_csv_row_fallback_name():
    row = ["ug_3", "", "", "False"]
    result = user_group_mgmt.TargetUserGroup.from_csv_row(row, "|")
    expected = user_group_mgmt.TargetUserGroup(
        user_group_id="ug_3",
        user_group_name="ug_3",
        parent_user_groups=[],
        is_active=False,
    )
    assert result == expected, "Row with empty name should fallback to user group ID"


def test_from_csv_row_invalid_is_active():
    row = ["ug_4", "Testers", "ug_1", "not_a_boolean"]
    result = user_group_mgmt.TargetUserGroup.from_csv_row(row, "|")
    expected = user_group_mgmt.TargetUserGroup(
        user_group_id="ug_4",
        user_group_name="Testers",
        parent_user_groups=["ug_1"],
        is_active=False,
    )
    assert result == expected, "Invalid 'is_active' value should default to False"


def prepare_sdk():
    def mock_list_user_groups():
        return [
            MockUserGroup("ug_1", "Admins", []).to_sdk(),
            MockUserGroup("ug_4", "TemporaryAccess", ["ug_2"]).to_sdk(),
        ]

    sdk = mock.Mock()
    sdk.catalog_user.list_user_groups = mock_list_user_groups
    return sdk


@mock.patch("scripts.user_group_mgmt.create_clients")
def test_user_group_mgmt_e2e(create_client):
    sdk = prepare_sdk()
    create_client.return_value = sdk

    args = argparse.Namespace(
        user_group_csv=TEST_CSV_PATH,
        delimiter=",",
        ug_delimiter="|",
        quotechar='"',
        verbose=False,
    )

    user_group_mgmt.user_group_mgmt(args)

    expected_create_or_update_calls = [
        mock.call(CatalogUserGroup.init("ug_2", "Developers", ["ug_1"])),
        mock.call(CatalogUserGroup.init("ug_3", "Testers", ["ug_1", "ug_2"])),
    ]
    sdk.catalog_user.create_or_update_user_group.assert_has_calls(
        expected_create_or_update_calls, any_order=True
    )

    expected_delete_calls = [mock.call("ug_4")]
    sdk.catalog_user.delete_user_group.assert_has_calls(
        expected_delete_calls, any_order=True
    )
