"""Articles API router."""
from fastapi import APIRouter, HTTPException, Query

from api.db import clickhouse
from api.schemas.article import ArticleListResponse, ArticleResponse

router = APIRouter()


@router.get("", response_model=ArticleListResponse)
def list_articles(
    section: str | None = Query(None, description="Filter by section"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    date_from: str | None = Query(None, description="Ingestion date from (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="Ingestion date to (YYYY-MM-DD)"),
):
    """List articles with optional filters and pagination."""
    try:
        rows, total = clickhouse.query_articles(
            section=section,
            limit=limit,
            offset=offset,
            date_from=date_from,
            date_to=date_to,
        )
        articles = [ArticleResponse(**row) for row in rows]
        return ArticleListResponse(
            articles=articles,
            total=total,
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{article_id}", response_model=ArticleResponse)
def get_article(article_id: str):
    """Get a single article by ID."""
    try:
        row = clickhouse.query_article_by_id(article_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Article not found")
        return ArticleResponse(**row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
