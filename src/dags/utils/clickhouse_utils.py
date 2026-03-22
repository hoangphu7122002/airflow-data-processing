"""ClickHouse utilities for VnExpress gold layer."""
import logging

VNEXPRESS_ARTICLES_DDL = """
CREATE TABLE IF NOT EXISTS default.vnexpress_articles (
    article_id String,
    url String,
    title String,
    section String,
    published_at Nullable(DateTime64(3)),
    summary String,
    body_text String,
    main_entities Array(String),
    tags Array(String),
    first_seen_at DateTime64(3),
    last_seen_at DateTime64(3),
    ingestion_date Date
) ENGINE = ReplacingMergeTree(last_seen_at)
PARTITION BY toYYYYMM(ingestion_date)
ORDER BY (section, last_seen_at, article_id);
"""


def create_vnexpress_articles_table(client) -> None:
    """Create vnexpress_articles table if not exists."""
    client.command(VNEXPRESS_ARTICLES_DDL.strip())


def insert_articles_df(
    client,
    df,
    table: str = "vnexpress_articles",
    database: str = "default",
) -> int:
    """
    Bulk insert DataFrame into ClickHouse vnexpress_articles table.

    Maps silver columns to ClickHouse schema. Ensures main_entities and tags
    are lists; converts published_at/ingestion_date to appropriate types.
    Returns number of rows inserted.
    """
    import pandas as pd

    if df.empty:
        return 0

    # Ensure array columns are lists (not strings)
    work = df.copy()
    for col in ("main_entities", "tags"):
        if col in work.columns:
            work[col] = work[col].apply(
                lambda x: list(x) if isinstance(x, (list, tuple)) else [] if pd.isna(x) else [str(x)]
            )
        else:
            work[col] = work.apply(lambda _: [], axis=1)

    # Ensure string columns
    for col in ("article_id", "url", "title", "section", "summary", "body_text"):
        if col in work.columns:
            work[col] = work[col].fillna("").astype(str)

    # Normalize datetime columns to timezone-naive UTC for ClickHouse DateTime64
    for col in ("first_seen_at", "last_seen_at"):
        if col in work.columns:
            ser = pd.to_datetime(work[col], utc=True)
            work[col] = ser.dt.tz_localize(None)

    # Normalize ingestion_date to date string YYYY-MM-DD
    if "ingestion_date" in work.columns:
        work["ingestion_date"] = pd.to_datetime(work["ingestion_date"]).dt.strftime("%Y-%m-%d")

    # Select and order columns to match ClickHouse schema
    cols = [
        "article_id", "url", "title", "section",
        "published_at", "summary", "body_text",
        "main_entities", "tags",
        "first_seen_at", "last_seen_at", "ingestion_date",
    ]
    missing = [c for c in cols if c not in work.columns]
    if missing:
        logging.warning("insert_articles_df: missing columns %s, filling with defaults", missing)
        for c in missing:
            if c in ("main_entities", "tags"):
                work[c] = [[] for _ in range(len(work))]
            elif c != "published_at":
                work[c] = ""
            else:
                work[c] = None

    work = work[cols]
    n = len(work)
    client.insert_df(table=table, database=database, df=work)
    logging.info("insert_articles_df: inserted %d rows into %s.%s", n, database, table)
    return n
