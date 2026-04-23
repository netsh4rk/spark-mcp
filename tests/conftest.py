"""Shared pytest fixtures.

These tests never touch a real Spark install: they exercise the security
invariants in isolation. Where a ``SparkDatabase`` instance is needed, we
bypass ``__init__`` with ``object.__new__`` so no SQLite file has to exist.
"""

from pathlib import Path

import pytest

from spark_mcp import database as db_module
from spark_mcp.database import SparkDatabase


@pytest.fixture
def fake_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point ``SPARK_CACHE`` at a temp dir so path-traversal tests have a
    well-defined jail root they can reason about.

    Returns the *resolved* cache path (matches what the code under test sees
    after ``.resolve()``).
    """
    cache = tmp_path / "spark_cache"
    cache.mkdir()
    monkeypatch.setattr(db_module, "SPARK_CACHE", cache)
    return cache.resolve()


@pytest.fixture
def db_instance() -> SparkDatabase:
    """A ``SparkDatabase`` with ``__init__`` bypassed.

    Use this when the method under test does not open a real SQLite file
    (path traversal checks, pure-Python helpers, fast-path parameter
    validation).
    """
    return object.__new__(SparkDatabase)
