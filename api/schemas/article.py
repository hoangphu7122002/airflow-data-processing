"""Pydantic schemas for VnExpress article API responses."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ArticleBase(BaseModel):
    """Base article fields."""

    article_id: str
    url: str
    title: str
    section: str = ""
    published_at: Optional[datetime] = None
    summary: str = ""
    body_text: str = ""
    main_entities: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    ingestion_date: Optional[str] = None


class ArticleResponse(ArticleBase):
    """Full article response."""

    pass


class ArticleListResponse(BaseModel):
    """Paginated list of articles."""

    articles: list[ArticleResponse]
    total: int
    limit: int
    offset: int
