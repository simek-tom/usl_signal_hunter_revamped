from pathlib import Path

from pydantic_settings import BaseSettings

_ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    supabase_url: str
    supabase_key: str

    leadspicker_api_key: str = ""
    leadspicker_base_url: str = "https://app.leadspicker.com"
    airtable_api_key: str = ""
    airtable_base_id: str = ""
    airtable_lp_general_table: str = "Leadspicker - general post"
    airtable_lp_czech_table: str = "Leadspicker - czehcia post"
    airtable_crunchbase_table: str = "Crunchbase Source"
    airtable_crunchbase_view: str = ""
    airtable_news_table: str = "Seed round"
    news_api_key: str = ""
    gemini_api_key: str = ""

    secret_key: str = "change-me"
    debug: bool = False
    # Comma-separated in .env; use the cors_origins property everywhere.
    cors_origins_str: str = "http://localhost:3000,http://localhost:5173,http://localhost:8000"

    model_config = {
        "env_file": str(_ENV_FILE),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins_str.split(",") if o.strip()]


settings = Settings()
