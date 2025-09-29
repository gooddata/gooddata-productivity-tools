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

import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
)

import argparse

import pytest
from gooddata_pipelines import UserGroupIncrementalLoad

from scripts import user_group_mgmt
from scripts.user_group_mgmt import read_users_groups_from_csv

TEST_CSV_PATH = "tests/data/user_group_mgmt/input.csv"


def test_conflicting_delimiters_raises_error(monkeypatch):
    monkeypatch.setattr("os.path.exists", lambda path: True)
    args = argparse.Namespace(
        user_group_csv="", delimiter=",", ug_delimiter=",", quotechar='"'
    )
    with pytest.raises(RuntimeError):
        user_group_mgmt.validate_args(args)


@pytest.fixture
def mock_read_csv_file_to_dict(mocker):
    """
    Fixture to mock read_csv_file_to_dict in scripts.user_group_mgmt.
    """

    def _mock(return_value):
        return mocker.patch(
            "scripts.user_group_mgmt.read_csv_file_to_dict",
            return_value=return_value,
        )

    return _mock


@pytest.mark.parametrize(
    "dict_row",
    [
        {
            "user_group_id": "ug_1",
            "user_group_name": "Admins",
            "parent_user_groups": "ug_2|ug_3",
            "is_active": "True",
        },
        {
            "user_group_id": "ug_2",
            "user_group_name": "Developers",
            "parent_user_groups": "",
            "is_active": "True",
        },
        {
            "user_group_id": "ug_3",
            "user_group_name": "",
            "parent_user_groups": "ug1",
            "is_active": "False",
        },
    ],
)
def test_from_csv_row_standard(mock_read_csv_file_to_dict, dict_row):
    mock_read_csv_file_to_dict([dict_row])
    result = read_users_groups_from_csv(
        argparse.Namespace(
            user_group_csv="", delimiter=",", ug_delimiter="|", quotechar='"'
        )
    )
    expected = [
        UserGroupIncrementalLoad(
            user_group_id=dict_row["user_group_id"],
            user_group_name=dict_row["user_group_name"] or dict_row["user_group_id"],
            parent_user_groups=(
                dict_row["parent_user_groups"].split("|")
                if dict_row["parent_user_groups"]
                else []
            ),
            is_active=dict_row["is_active"],
        )
    ]
    assert result == expected
