"""Standalone CLI shim kept for direct `python glacier_cli.py` usage."""

from __future__ import annotations

from glacier_analysis.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
