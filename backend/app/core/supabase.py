from supabase import AsyncClient, acreate_client

from app.core.config import settings

_client: AsyncClient | None = None


async def get_supabase() -> AsyncClient:
    """FastAPI dependency — returns a lazily-initialized Supabase async client."""
    global _client
    if _client is None:
        _client = await acreate_client(settings.supabase_url, settings.supabase_key)
    return _client
