from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payments.api.routes import router
from payments.config import ApiSettings
from payments.db.session import create_engine, create_session_factory


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = settings if settings is not None else ApiSettings()
        engine = create_engine(app.state.settings)
        app.state.session_factory = create_session_factory(engine)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="Payments API", lifespan=lifespan)
    app.include_router(router)
    return app
