# (C) 2025 GoodData Corporation
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
)


import argparse
from unittest import mock

import pytest

from scripts import user_mgmt


@mock.patch("os.path.exists")
def test_conflicting_delimiters_raises_error(path_exists):
    path_exists.return_value = True
    args = argparse.Namespace(
        conf="", user_csv="", delimiter=",", ug_delimiter=",", quotechar='"'
    )
    with pytest.raises(RuntimeError):
        user_mgmt.validate_args(args)
