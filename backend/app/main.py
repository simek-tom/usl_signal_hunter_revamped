from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from app.core.config import settings
from app.core.runtime_config import load_runtime_env_overrides
from app.core.supabase import get_supabase
from app.api import health
from app.api import settings_api, lp_projects, blacklist, import_lp, batches, entries, search, drafting, push, ai, pipeline_configs, staging


def create_app() -> FastAPI:
    app = FastAPI(
        title="USL Signal Hunter",
        version="2.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router, prefix="/api")
    app.include_router(settings_api.router, prefix="/api")
    app.include_router(lp_projects.router, prefix="/api")
    app.include_router(blacklist.router, prefix="/api")
    app.include_router(import_lp.router, prefix="/api")
    app.include_router(batches.router, prefix="/api")
    app.include_router(pipeline_configs.router, prefix="/api")
    app.include_router(staging.router, prefix="/api")
    app.include_router(entries.router, prefix="/api")
    app.include_router(search.router, prefix="/api")
    app.include_router(drafting.router, prefix="/api")
    app.include_router(push.router, prefix="/api")
    app.include_router(ai.router, prefix="/api")

    @app.on_event("startup")
    async def startup_load_runtime_overrides():
        # Best-effort: service still works with .env defaults if this fails.
        try:
            db = await get_supabase()
            await load_runtime_env_overrides(db)
        except Exception:
            pass

    static_dir = Path(__file__).resolve().parent / "static"
    index_file = static_dir / "index.html"

    @app.get("/", include_in_schema=False)
    async def spa_root():
        if not index_file.exists():
            raise HTTPException(
                status_code=404,
                detail="Frontend index not found. Build frontend into backend/app/static.",
            )
        return FileResponse(index_file)

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_files(full_path: str):
        # Keep non-existent API routes as 404 API errors, not SPA fallback.
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="API route not found")

        requested = (static_dir / full_path).resolve()
        static_root = static_dir.resolve()
        if not str(requested).startswith(str(static_root)):
            raise HTTPException(status_code=404, detail="Not found")

        if requested.is_file():
            return FileResponse(requested)

        if index_file.exists():
            return FileResponse(index_file)

        raise HTTPException(
            status_code=404,
            detail="Frontend not built. Run `npm run build` in frontend/.",
        )

    return app
