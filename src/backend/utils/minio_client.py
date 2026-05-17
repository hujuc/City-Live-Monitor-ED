"""MinIO helper — uploads local Parquet files to the S3-compatible lakehouse.

Fail-soft by design: if MinIO is unreachable the caller gets a False return
and a warning log, but the pipeline continues writing to local FS.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from config.settings import Config


def _get_client():
    try:
        from minio import Minio

        return Minio(
            Config.MINIO_ENDPOINT,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            secure=Config.MINIO_SECURE,
        )
    except ImportError:
        return None


def ensure_bucket(bucket: str) -> bool:
    """Create bucket if it does not exist. Returns False if MinIO disabled or unavailable."""
    if not Config.MINIO_ENABLED:
        return False
    client = _get_client()
    if client is None:
        return False
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info("[minio] Created bucket '{}'", bucket)
        return True
    except Exception as exc:
        logger.warning("[minio] Cannot reach MinIO at {}: {}", Config.MINIO_ENDPOINT, exc)
        return False


def upload_file(local_path: Path, bucket: str, object_key: str) -> bool:
    """Upload a local file to MinIO. Returns False if MinIO disabled or unavailable."""
    if not Config.MINIO_ENABLED:
        logger.debug("[minio] MINIO_ENABLED=false — skipping upload of {}", local_path.name)
        return False
    client = _get_client()
    if client is None:
        logger.debug("[minio] minio package not installed — skipping upload of {}", local_path.name)
        return False
    try:
        ensure_bucket(bucket)
        client.fput_object(bucket, object_key, str(local_path))
        logger.info("[minio] Uploaded s3://{}/{}", bucket, object_key)
        return True
    except Exception as exc:
        logger.warning("[minio] Upload failed for {}: {}", object_key, exc)
        return False
