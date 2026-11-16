import asyncio

from alembic import context
from alembic.util import CommandError
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import create_async_engine

from payments.config import get_settings
from payments.db.base import Base
from payments.db.models import OutboxEvent, Payment  # noqa: F401

target_metadata = Base.metadata


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(
        get_settings().database_url.get_secret_value(), poolclass=pool.NullPool
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    raise CommandError("Offline migrations are not supported. Run Alembic without --sql.")

asyncio.run(run_migrations_online())
