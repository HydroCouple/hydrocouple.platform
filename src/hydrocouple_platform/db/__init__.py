"""Database engine, sessions and migrations for the control plane."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def make_engine(url: str) -> Engine:
    if not url.startswith("postgresql"):
        raise ValueError("the control plane requires PostgreSQL (postgresql+psycopg://...)")
    return create_engine(url, pool_pre_ping=True, future=True)


def make_sessionmaker(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False, future=True)


@contextmanager
def transaction(factory: sessionmaker[Session]) -> Iterator[Session]:
    """One unit of work: commit on success, roll back on any exception."""
    session = factory()
    try:
        with session.begin():
            yield session
    finally:
        session.close()


def upgrade(url: str, revision: str = "head") -> None:
    """Apply migrations; deployments run this before starting any service."""
    from alembic import command
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.upgrade(cfg, revision)
