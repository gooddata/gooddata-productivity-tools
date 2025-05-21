from pydantic import BaseModel  # type: ignore[import] # missing type stub


class Page(BaseModel):
    size: int
    totalElements: int
    totalPages: int
    number: int


class Hierarchy(BaseModel):
    childrenCount: int


class Meta(BaseModel):
    page: Page | None = None
    hierarchy: Hierarchy | None = None


class Workspace(BaseModel):
    id: str
    meta: Meta | None = None


class Links(BaseModel):
    self: str
    next: str | None = None


class WorkspaceResponse(BaseModel):
    data: list[Workspace]
    links: Links
    meta: Meta
