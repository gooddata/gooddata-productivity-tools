# (C) 2025 GoodData Corporation
"""This module defines type aliases intended to improve readability."""

from typing import TypeAlias

_WorkspaceId: TypeAlias = str
_DatasetId: TypeAlias = str
_RawData: TypeAlias = list[dict[str, str]]

__all__ = ["_WorkspaceId", "_DatasetId", "_RawData"]
