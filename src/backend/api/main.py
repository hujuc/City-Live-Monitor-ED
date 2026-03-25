"""
FastAPI REST Server

Serves analytics-ready data from the Silver/Gold layers.
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from config.settings import Config
from backend.storage.silver import SilverStorage

app = FastAPI(
    title="City Live Monitor API",
    description="City live monitoring — batch and streaming analytics",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_silver = SilverStorage()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
