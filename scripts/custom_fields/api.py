# (C) 2025 GoodData Corporation
import json
from typing import Any

import requests
from gooddata_sdk.sdk import GoodDataSdk

TIMEOUT = 60
PANTHER_API_VERSION = "v1"


class GoodDataAPI:
    headers: dict[str, str]
    base_url: str

    def __init__(self, host: str, token: str) -> None:
        """Initialize the GoodDataAPI with host and token.

        Args:
            host (str): The GoodData Cloud host URL.
            token (str): The authentication token for the GoodData Cloud API.
        """
        self._domain: str = host
        self._token: str = token

        # Initialize the GoodData SDK
        self._sdk = GoodDataSdk.create(self._domain, self._token)

        # Set up utils for direct API interaction
        self.base_url = self._get_base_url(self._domain)
        self.headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/vnd.gooddata.api+json",
        }

    def get_workspace_layout(self, workspace_id: str) -> requests.Response:
        """Get the layout of the specified workspace.

        Args:
            workspace_id (str): The ID of the workspace to retrieve the layout for.
        Returns:
            requests.Response: The response containing the workspace layout.
        """
        endpoint = f"/layout/workspaces/{workspace_id}"
        return self._get(endpoint)

    def put_workspace_layout(
        self, workspace_id: str, layout: dict[str, Any]
    ) -> requests.Response:
        """Update the layout of the specified workspace.

        Args:
            workspace_id (str): The ID of the workspace to update.
            layout (dict[str, Any]): The new layout to set for the workspace.
        Returns:
            requests.Response: The response from the server after updating the layout.
        """
        endpoint = f"/layout/workspaces/{workspace_id}"
        headers = {**self.headers, "Content-Type": "application/json"}
        return self._put(endpoint, data=layout, headers=headers)

    def get_all_metrics(self, workspace_id: str) -> requests.Response:
        """Get all metrics from the specified workspace.

        Args:
            workspace_id (str): The ID of the workspace to retrieve metrics from.
        Returns:
            requests.Response: The response containing the metrics.
        """
        endpoint = f"/entities/workspaces/{workspace_id}/metrics"
        headers = {**self.headers, "X-GDC-VALIDATE-RELATIONS": "true"}
        return self._get(endpoint, headers=headers)

    def get_all_visualization_objects(self, workspace_id: str) -> requests.Response:
        """Get all visualizations from the specified workspace.

        Args:
            workspace_id (str): The ID of the workspace to retrieve visualizations from.
        Returns:
            requests.Response: The response containing the visualizations.
        """
        endpoint = f"/entities/workspaces/{workspace_id}/visualizationObjects"
        headers = {**self.headers, "X-GDC-VALIDATE-RELATIONS": "true"}
        return self._get(endpoint, headers=headers)

    def get_all_dashboards(self, workspace_id: str) -> requests.Response:
        """Get all dashboards from the specified workspace.

        Args:
            workspace_id (str): The ID of the workspace to retrieve dashboards from.
        Returns:
            requests.Response: The response containing the dashboards.
        """
        endpoint = f"/entities/workspaces/{workspace_id}/analyticalDashboards"
        headers = {**self.headers, "X-GDC-VALIDATE-RELATIONS": "true"}
        return self._get(endpoint, headers=headers)

    @staticmethod
    def _get_base_url(domain: str) -> str:
        """Returns the root endpoint for the GoodData Cloud API.

        Method ensures that the URL starts with "https://" and does not
        end with a trailing slash.

        Args:
            domain (str): The domain of the GoodData Cloud instance.
        Returns:
            str: The base URL for the GoodData Cloud API.
        """
        # Remove trailing slash if present.
        if domain[-1] == "/":
            domain = domain[:-1]

        if not domain.startswith("https://") and not domain.startswith("http://"):
            domain = f"https://{domain}"

        if domain.startswith("http://") and not domain.startswith("https://"):
            domain = domain.replace("http://", "https://")

        return f"{domain}/api/{PANTHER_API_VERSION}"

    def _get_url(self, endpoint: str) -> str:
        """Returns the full URL for a given API endpoint.

        Args:
            endpoint (str): The API endpoint to be appended to the base URL.
        Returns:
            str: The full URL for the API endpoint.
        """
        return f"{self.base_url}{endpoint}"

    def _get(
        self, endpoint: str, headers: dict[str, str] | None = None
    ) -> requests.Response:
        """Sends a GET request to the server.

        Args:
            endpoint (str): The API endpoint to send the GET request to.
        Returns:
            requests.Response: The response from the server.
        """
        url = self._get_url(endpoint)
        request_headers = headers if headers else self.headers

        return requests.get(url, headers=request_headers, timeout=TIMEOUT)

    def _put(
        self,
        endpoint: str,
        data: Any,
        headers: dict | None = None,
    ) -> requests.Response:
        """Sends a PUT request to the server with a given JSON object.

        Args:
            endpoint (str): The API endpoint to send the PUT request to.
            data (Any): The JSON data to send in the request body.
            headers (dict | None): Headers to include in the request.
                If no headers are provided, the default headers will be used.
        Returns:
            requests.Response: The response from the server.
        """
        url = self._get_url(endpoint)
        request_headers = headers if headers else self.headers
        data_json = json.dumps(data)

        return requests.put(
            url, data=data_json, headers=request_headers, timeout=TIMEOUT
        )

    @staticmethod
    def raise_if_response_not_ok(*responses: requests.Response) -> None:
        """Check if responses from API calls are OK.

        Raises ValueError if any response is not OK (status code not 2xx).
        """
        for response in responses:
            if not response.ok:
                raise ValueError(
                    f"Request to {response.url} failed with status code {response.status_code}: {response.text}"
                )
