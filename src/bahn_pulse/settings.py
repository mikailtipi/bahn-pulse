"""Runtime settings from environment variables (and a local, git-ignored .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(path: Path = Path(".env")) -> None:
    """Load KEY=VALUE lines from a .env file into os.environ (existing values win)."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing environment variable {name} (see .env.example)")
    return value


@dataclass(frozen=True)
class DbApiCredentials:
    client_id: str
    api_key: str

    @classmethod
    def from_env(cls) -> DbApiCredentials:
        return cls(client_id=_require("DB_CLIENT_ID"), api_key=_require("DB_API_KEY"))

    def __repr__(self) -> str:  # never print secrets
        return "DbApiCredentials(client_id=***, api_key=***)"


def bronze_bucket() -> str:
    return _require("BRONZE_BUCKET")
