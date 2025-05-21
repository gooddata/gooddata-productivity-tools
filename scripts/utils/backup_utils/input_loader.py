# (C) 2025 GoodData Corporation
import csv
import logging
from dataclasses import dataclass
from typing import Iterator

from utils.gd_api import (  # type: ignore[import]
    API_VERSION,
    GDApi,
    GoodDataRestApiError,
    MaybeResponse,
)
from utils.models.batch import Size  # type: ignore[import]
from utils.models.workspace_response import (  # type: ignore[import]
    Workspace,
    WorkspaceResponse,
)

logger = logging.getLogger(__name__)


class InputLoader:
    """Class to handle loading and parsing the input data."""

    api_client: GDApi
    base_workspace_endpoint: str
    hierarchy_endpoint: str
    all_workspaces_endpoint: str

    def __init__(self, api_client: GDApi, page_size: Size) -> None:
        self.api_client = api_client
        self.page_size = page_size.size
        self.set_endpoints()

    def set_endpoints(self) -> None:
        """Sets the hierarchy endpoint for the API client."""
        self.base_workspace_endpoint = "/api/v1/entities/workspaces"
        self.hierarchy_endpoint = (
            f"{self.base_workspace_endpoint}?"
            + "filter=parent.id=={parent_id}"
            + f"&include=parent&page=0&size={self.page_size}&sort=name,asc&metaInclude=page,hierarchy"
        )
        self.all_workspaces_endpoint = f"{self.base_workspace_endpoint}?page=0&size={self.page_size}&sort=name,asc&metaInclude=page"

    @dataclass
    class _ProcessDataOutput:
        workspace_ids: list[str]
        sub_parents: list[str] | None = None

    @staticmethod
    def read_csv_input_for_backup(file_path: str) -> list[str]:
        """Reads the input CSV file and returns its content from the first column as a list of string."""

        with open(file_path) as csv_file:
            reader: Iterator[list[str]] = csv.reader(csv_file, skipinitialspace=True)

            try:
                # Skip the header
                headers = next(reader)

                if len(headers) > 1:
                    raise ValueError(
                        "Input file contains more than one column. Please check the input and try again."
                    )

            except StopIteration:
                # Raise an error if the iterator is empty
                raise ValueError("No content found in the CSV file.")

            # Read the content
            content = [row[0] for row in reader]

            # If the content is empty (no rows), raise an error
            if not content:
                raise ValueError("No workspaces found in the CSV file.")

        return content

    def fetch_page(self, url: str) -> WorkspaceResponse:
        """Fetch a page of workspaces."""

        # Separate the API path from the URL so that it can be fed to the GDApi class
        endpoint: str = url.split(f"api/{API_VERSION}")[1]
        response: MaybeResponse = self.api_client.get(endpoint, None)
        if response:
            return WorkspaceResponse(**response.json())
        else:
            raise GoodDataRestApiError(
                f"Failed to fetch data from the API. URL: {endpoint}"
            )

    @staticmethod
    def process_data(data: list[Workspace]) -> _ProcessDataOutput:
        """Extract children and sub-parents from workspace data."""
        children: list[str] = []
        sub_parents: list[str] = []

        for workspace in data:
            # append child workspace IDs
            children.append(workspace.id)

            # if hierarchy is present and has children, append child workspace ID to sub_parents
            if workspace.meta and workspace.meta.hierarchy:
                if workspace.meta.hierarchy.children_count > 0:
                    sub_parents.append(workspace.id)
        return InputLoader._ProcessDataOutput(children, sub_parents)

    @staticmethod
    def log_paging_progress(response: WorkspaceResponse) -> None:
        """Log the progress of paging through API responses if paginatino data is present"""
        current_page: int | None
        total_pages: int | None

        if response.meta.page:
            current_page = response.meta.page.number + 1
            total_pages = response.meta.page.total_pages
        else:
            current_page = None
            total_pages = None

        if current_page and total_pages:
            logger.info(f"Fetched page: {current_page} of {total_pages}")

    def _paginate(self, url: str | None):
        result: list[InputLoader._ProcessDataOutput] = []
        while url:
            response: WorkspaceResponse = self.fetch_page(url)
            self.log_paging_progress(response)
            result.append(self.process_data(response.data))
            url = response.links.next

        return result

    def get_hierarchy(self, parent_id: str) -> list[str]:
        """Returns a list of workspace IDs in the hierarchy."""
        logger.info(f"Fetching children of {parent_id}")
        url = self.hierarchy_endpoint.format(parent_id=parent_id)

        all_children, sub_parents = [], []

        results: list[InputLoader._ProcessDataOutput] = self._paginate(url)

        for result in results:
            all_children.extend(result.workspace_ids)
            if result.sub_parents:
                sub_parents.extend(result.sub_parents)

        for subparent in sub_parents:
            all_children += self.get_hierarchy(subparent)

        if not all_children:
            logger.warning(
                f"No child workspaces found for parent workspace ID: {parent_id}"
            )

        return all_children

    def get_all_workspaces(self) -> list[str]:
        """Returns a list of all workspace IDs in the organization."""
        # TODO: can be optimized - requests can be sent asynchronously.
        # Use the total number of pages to calculate the number of requests
        # to be sent. Use semaphore or otherwise limit the number of concurrent
        # requests to avoid putting too much load on the server.
        logger.info("Fetching all workspaces")
        url = self.all_workspaces_endpoint

        all_workspaces: list[str] = []

        results: list[InputLoader._ProcessDataOutput] = self._paginate(url)

        for result in results:
            all_workspaces.extend(result.workspace_ids)

        if not all_workspaces:
            logger.warning("No workspaces found in the organization.")

        return all_workspaces

    def get_ids_to_backup(self, input_type: str, path_to_csv: str) -> list[str]:
        """Returns the list of workspace IDs to back up based on the input type."""
        if input_type == "list-of-workspaces":
            return self.read_csv_input_for_backup(path_to_csv)

        else:
            if input_type == "list-of-parents":
                list_of_parents = self.read_csv_input_for_backup(path_to_csv)
                list_of_children: list[str] = []

                for parent in list_of_parents:
                    list_of_children.extend(self.get_hierarchy(parent))

                # Include the parent workspaces in the backup
                return list_of_parents + list_of_children

            if input_type == "entire-organization":
                list_of_workspaces = self.get_all_workspaces()
                return list_of_workspaces

        raise RuntimeError("Invalid input type provided.")
