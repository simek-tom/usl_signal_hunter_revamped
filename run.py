import os
import sys
import threading
import webbrowser
from pathlib import Path

# Make the backend package importable when running from the project root.
sys.path.insert(0, str(Path(__file__).parent / "backend"))

import uvicorn

from app.main import create_app

app = create_app()


def _to_browser_host(host: str) -> str:
    if host in {"0.0.0.0", "::", ""}:
        return "localhost"
    # Wrap bare IPv6 hosts for URL safety.
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0").strip()
    port = int(os.getenv("PORT", "8000"))
    reload = _bool_env("RELOAD", True)
    auto_open = _bool_env("AUTO_OPEN_BROWSER", True)

    if auto_open:
        url = f"http://{_to_browser_host(host)}:{port}"
        # Run in the background so server startup is not blocked.
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    uvicorn.run("run:app", host=host, port=port, reload=reload)
