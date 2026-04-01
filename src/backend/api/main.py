"""
FastAPI REST Server

Serves analytics-ready data from the Silver/Gold layers.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
