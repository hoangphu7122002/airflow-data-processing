"""FastAPI application for VnExpress article API."""
from fastapi import FastAPI

from api.routers import articles, trigger

app = FastAPI(
    title="VnExpress API",
    description="REST API for VnExpress crawled article data",
    version="0.1.0",
)
app.include_router(articles.router, prefix="/articles", tags=["articles"])
app.include_router(trigger.router, tags=["trigger"])


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok", "version": "0.1.0"}
