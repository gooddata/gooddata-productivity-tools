# (C) 2023 GoodData Corporation

import json
from typing import Any, TypeAlias

import requests
from utils.logger import logger  # type: ignore[import]

API_VERSION = "v1"
BEARER_TKN_PREFIX = "Bearer"

MaybeResponse: TypeAlias = requests.Response | None


class GoodDataRestApiError(Exception):
    """Wrapper for errors occurring from interaction with GD REST API."""


class GDApi:
    """Wrapper for GoodData REST API client."""

    # TODO: also defined in restore.py, consider moving to utils
    def __init__(self, host: str, api_token: str, headers=None):
        self.endpoint = self._handle_endpoint(host)
        self.api_token = api_token
        self.headers = headers if headers else {}
        self.wait_api_time = 10

    @staticmethod
    def _handle_endpoint(host: str) -> str:
        """Ensures that the endpoint URL is correctly formatted."""
        return (
            f"{host}api/{API_VERSION}"
            if host[-1] == "/"
            else f"{host}/api/{API_VERSION}"
        )

    def get(
        self,
        path: str,
        params,
        ok_code: int = 200,
        not_found_code: int = 404,
    ) -> MaybeResponse:
        """Sends a GET request to the GoodData API."""
        kwargs = self._prepare_request(path, params)
        logger.debug(f"GET request: {json.dumps(kwargs)}")
        response = requests.get(**kwargs)
        return self._resolve_return_code(
            response, ok_code, kwargs["url"], "RestApi.get", not_found_code
        )

    def _prepare_request(self, path: str, params=None) -> dict[str, Any]:
        """Prepares the request to be sent to the GoodData API."""
        kwargs: dict[str, Any] = {
            "url": f"{self.endpoint}/{path}",
            "headers": self.headers.copy(),
        }
        if params:
            kwargs["params"] = params
        if self.api_token:
            kwargs["headers"]["Authorization"] = f"{BEARER_TKN_PREFIX} {self.api_token}"
        else:
            raise RuntimeError(
                "Token required for authentication against GD API is missing."
            )
        # TODO - Currently no credentials validation
        # TODO - do we also support username+pwd auth? Or do we enforce token only?
        # else:
        #     kwargs['auth'] = (self.user, self.password) if self.user is not None else None  # noqa
        return kwargs

    @staticmethod
    def _resolve_return_code(
        response, ok_code: int, url, method, not_found_code: int | None = None
    ) -> MaybeResponse:
        """Resolves the return code of the response."""
        if response.status_code == ok_code:
            logger.debug(f"{method} to {url} succeeded")
            return response
        if not_found_code and response.status_code == not_found_code:
            logger.debug(f"{method} to {url} failed - target not found")
            return None
        raise GoodDataRestApiError(
            f"{method} to {url} failed - "
            f"response_code={response.status_code} message={response.text}"
        )
