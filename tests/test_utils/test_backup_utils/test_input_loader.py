# (C) 2025 GoodData Corporation
import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../scripts"))
)

import tempfile

import pytest

from scripts.utils.backup_utils.input_loader import InputLoader
from scripts.utils.gd_api import GDApi
from scripts.utils.models.workspace_response import (
    Hierarchy,
    Links,
    Meta,
    Page,
    Workspace,
    WorkspaceResponse,
)

MOCK_GDP_API = GDApi(
    host="https://fake.host/",
    api_token="fake_token",
)

# MOCK_INPUT_LOADER = backup.InputLoader(MOCK_GD_API, 100)


@pytest.fixture
def input_loader():
    loader = InputLoader(MOCK_GDP_API, page_size=2)
    loader.hierarchy_endpoint = "/fake/hierarchy?filter=parent.id=={parent_id}"
    loader.all_workspaces_endpoint = "/fake/all"
    return loader


def test_process_data_extracts_children_and_subparents():
    ws1 = Workspace(id="ws1", meta=Meta(hierarchy=Hierarchy(childrenCount=2)))
    ws2 = Workspace(id="ws2", meta=Meta(hierarchy=Hierarchy(childrenCount=0)))
    ws3 = Workspace(id="ws3", meta=None)

    result = InputLoader.process_data([ws1, ws2, ws3])
    assert result.workspace_ids == ["ws1", "ws2", "ws3"]
    assert result.sub_parents == ["ws1"]


def test_log_paging_progress_logs_info(mocker):
    response = WorkspaceResponse(
        data=[],
        meta=Meta(
            page=Page(size=5, totalElements=25, number=1, totalPages=5), hierarchy=None
        ),
        links=Links(self="self", next="next"),
    )

    mock_logger = mocker.patch("scripts.utils.logger.logger.info")
    InputLoader.log_paging_progress(response)
    mock_logger.assert_called_once


def test_log_paging_progress_no_page(mocker):
    response = WorkspaceResponse(
        data=[],
        meta=Meta(page=None, hierarchy=None),
        links=Links(self="self", next="next"),
    )

    mock_logger = mocker.patch("scripts.utils.logger.logger.info")
    InputLoader.log_paging_progress(response)
    assert mock_logger.call_count == 0


def test_paginate_calls_fetch_page_and_process_data(input_loader, monkeypatch):
    ws1 = Workspace(id="ws1", meta=Meta(hierarchy=Hierarchy(childrenCount=1)))
    ws2 = Workspace(id="ws2", meta=Meta(hierarchy=Hierarchy(childrenCount=0)))
    links1 = Links(self="self", next="next_url")
    links2 = Links(self="self", next=None)
    resp1 = WorkspaceResponse(
        data=[ws1], meta=Meta(hierarchy=None, page=None), links=links1
    )
    resp2 = WorkspaceResponse(
        data=[ws2], meta=Meta(hierarchy=None, page=None), links=links2
    )

    fetch_page_calls = []

    def fetch_page_side_effect(url):
        fetch_page_calls.append(url)
        return resp1 if len(fetch_page_calls) == 1 else resp2

    input_loader.fetch_page = fetch_page_side_effect

    process_data_calls = []

    def process_data_side_effect(data):
        process_data_calls.append(data)
        if len(process_data_calls) == 1:
            return InputLoader._ProcessDataOutput(["ws1"], ["ws1"])
        else:
            return InputLoader._ProcessDataOutput(["ws2"], [])

    monkeypatch.setattr(
        InputLoader, "process_data", staticmethod(process_data_side_effect)
    )
    monkeypatch.setattr(
        InputLoader, "log_paging_progress", staticmethod(lambda resp: None)
    )

    result = input_loader._paginate("first_url")
    assert len(result) == 2
    assert result[0].workspace_ids == ["ws1"]
    assert result[1].workspace_ids == ["ws2"]
    assert len(fetch_page_calls) == 2
    assert len(process_data_calls) == 2


def test_get_hierarchy_recurses(input_loader, monkeypatch):
    def fake_paginate(url):
        if "p1" in url:
            return [InputLoader._ProcessDataOutput(["c1"], ["c1"])]
        if "c1" in url:
            return [InputLoader._ProcessDataOutput(["c2"], [])]
        return []

    input_loader._paginate = fake_paginate
    monkeypatch.setattr(
        "scripts.utils.backup_utils.input_loader.logger",
        type("Logger", (), {"info": lambda self, msg: None})(),
    )
    result = input_loader.get_hierarchy("p1")
    assert set(result) == {"c1", "c2"}


def test_get_workspaces_to_backup_empty_org(input_loader, monkeypatch):
    monkeypatch.setattr(
        input_loader,
        "get_all_workspaces",
        lambda: [],
    )
    with pytest.raises(RuntimeError, match="No workspaces found in the organization."):
        input_loader.get_ids_to_backup(
            "entire-organization",
            "some-csv-file.csv",
        )


def test_get_workspaces_to_backup_wrong_input_type(input_loader):
    with pytest.raises(RuntimeError, match="Invalid input type provided."):
        input_loader.get_ids_to_backup(
            "invalid-input-type",
            "some-csv-file.csv",
        )


def test_read_csv_input_empty_file(input_loader) -> None:
    """Test with an empty CSV file."""
    with tempfile.NamedTemporaryFile() as temp_csv:
        path_to_csv = temp_csv.name
        with pytest.raises(ValueError, match="No content found in the CSV file."):
            input_loader.read_csv_input_for_backup(path_to_csv)


def test_read_csv_input_only_header(input_loader) -> None:
    """Test with a CSV file that contains only the header."""
    with tempfile.NamedTemporaryFile() as temp_csv:
        temp_csv.write(b"header1\n")
        temp_csv.flush()
        temp_csv.seek(0)
        path_to_csv = temp_csv.name
        with pytest.raises(ValueError, match="No workspaces found in the CSV file."):
            input_loader.read_csv_input_for_backup(path_to_csv)


def test_read_csv_input_valid(input_loader) -> None:
    """Test with a valid CSV file."""
    with tempfile.NamedTemporaryFile(delete=False) as temp_csv:
        temp_csv.write(b"header1\n")
        temp_csv.write(b"workspace1\n")
        temp_csv.write(b"workspace2\n")
        temp_csv.flush()
        temp_csv.seek(0)
        path_to_csv = temp_csv.name
        result = input_loader.read_csv_input_for_backup(path_to_csv)
        assert result == ["workspace1", "workspace2"]
    os.remove(path_to_csv)


def test_read_csv_input_too_many_columns(input_loader) -> None:
    """Test with a CSV file that contains too many columns."""
    with tempfile.NamedTemporaryFile(delete=False) as temp_csv:
        temp_csv.write(b"header1,header2\n")
        temp_csv.write(b"workspace1,extra_column\n")
        temp_csv.flush()
        temp_csv.seek(0)
        path_to_csv = temp_csv.name
        with pytest.raises(
            ValueError,
            match="Input file contains more than one column. Please check the input and try again.",
        ):
            input_loader.read_csv_input_for_backup(path_to_csv)
    os.remove(path_to_csv)
