from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, Field  # type: ignore[import]


@dataclass
class BackupBatch:
    list_of_ids: list[str]


class Size(BaseModel):
    """Model to ensure valid batch or page size, i.e., integer greater than 0."""

    size: Annotated[int, Field(gt=0, description="Batch size must be greater than 0")]
