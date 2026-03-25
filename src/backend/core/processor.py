"""Abstract base class for all data processors."""

from abc import ABC, abstractmethod
from pathlib import Path


class BaseProcessor(ABC):
    """
    Abstract base for batch and stream processors.
    Implementations read from Bronze, apply transformations, and write to Silver/Gold.
    """

    @property
    @abstractmethod
    def processor_name(self) -> str:
        """Unique identifier for this processor (e.g. 'batch_processor')."""

    @abstractmethod
    def process(self, source: Path, destination: Path) -> int:
        """
        Read raw data from source, transform it, and write to destination.

        Returns:
            int: Number of records processed.
        """
