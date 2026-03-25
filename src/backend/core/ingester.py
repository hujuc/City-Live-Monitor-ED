"""Abstract base class for all data ingesters."""

from abc import ABC, abstractmethod
from pathlib import Path


class BaseIngester(ABC):
    """
    Abstract base for both batch and streaming ingesters.
    Implementations fetch raw data and write it to the Bronze layer.
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this data source (e.g. 'batch_source', 'stream_source')."""

    @abstractmethod
    def ingest(self, destination: Path | None = None) -> int:
        """
        Fetch data from the source and persist raw records to destination.

        Returns:
            int: Number of records or files successfully ingested.
        """
