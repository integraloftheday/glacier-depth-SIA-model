"""Core glacier analysis package."""

from .cli_runner import run_pipeline
from .config import PipelineConfig

__all__ = ["PipelineConfig", "run_pipeline"]
