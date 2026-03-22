"""ClickHouse client for querying vnexpress_articles."""
import os
from typing import Any

import clickhouse_connect


def get_client():
    """Get ClickHouse client using environment variables."""
    host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    username = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    return clickhouse_connect.get_client(
        host=host, port=port, username=username, password=password
    )


def query_articles(
    *,
    section: str | None = None,
    limit: int = 20,
    offset: int = 0,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    Query articles from vnexpress_articles with filters.
    Returns (list of row dicts, total count).
    """
    client = get_client()

    conditions = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if section:
        conditions.append("section = %(section)s")
        params["section"] = section
    if date_from:
        conditions.append("ingestion_date >= %(date_from)s")
        params["date_from"] = date_from
    if date_to:
        conditions.append("ingestion_date <= %(date_to)s")
        params["date_to"] = date_to

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"SELECT count() FROM default.vnexpress_articles WHERE {where_clause}"
    count_result = client.query(count_sql, parameters=params)
    total = count_result.result_rows[0][0] if count_result.result_rows else 0

    select_sql = f"""
        SELECT article_id, url, title, section, published_at, summary, body_text,
               main_entities, tags, first_seen_at, last_seen_at, ingestion_date
        FROM default.vnexpress_articles
        WHERE {where_clause}
        ORDER BY last_seen_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    result = client.query(select_sql, parameters=params)
    columns = result.column_names
    rows = [dict(zip(columns, row)) for row in result.result_rows]

    return rows, total


def query_article_by_id(article_id: str) -> dict[str, Any] | None:
    """Fetch a single article by article_id."""
    client = get_client()
    result = client.query(
        """
        SELECT article_id, url, title, section, published_at, summary, body_text,
               main_entities, tags, first_seen_at, last_seen_at, ingestion_date
        FROM default.vnexpress_articles
        WHERE article_id = %(article_id)s
        LIMIT 1
        """,
        parameters={"article_id": article_id},
    )
    if not result.result_rows:
        return None
    columns = result.column_names
    return dict(zip(columns, result.result_rows[0]))
