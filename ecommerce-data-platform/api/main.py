"""
FastAPI application entry point for the serving layer.
Data Lineage: PostgreSQL/dbt marts -> api/main.py -> REST clients, dashboard
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from api.routers import metrics, products, users
from api.dependencies import init_db_pool, close_db_pool
from configs.logging_config import get_logger

logger = get_logger(__name__, "api.log")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and teardown resources on startup/shutdown."""
    logger.info("Starting API server — initializing DB pool...")
    await init_db_pool()
    yield
    logger.info("Shutting down API server — closing DB pool...")
    await close_db_pool()


app = FastAPI(
    title="E-Commerce Data Platform API",
    description=(
        "REST API serving analytics from the e-commerce data warehouse. "
        "Provides top products, user activity, and sales metrics."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Middleware ─────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ── Routers ────────────────────────────────────────────────────────────────

app.include_router(products.router, prefix="/api/v1/products", tags=["Products"])
app.include_router(users.router, prefix="/api/v1/users", tags=["Users"])
app.include_router(metrics.router, prefix="/api/v1/metrics", tags=["Metrics"])


# ── Health & Root ──────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "service": "ecommerce-data-platform-api", "version": "1.0.0"}


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy"}


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        workers=int(os.getenv("API_WORKERS", "4")),
        reload=False,
        log_level="info",
    )
