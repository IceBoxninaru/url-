from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = ROOT_DIR / "app"
DEFAULT_SETTINGS_MODULE = "config.settings"


def configure_django(settings_module: str = DEFAULT_SETTINGS_MODULE) -> None:
    if str(APP_DIR) not in sys.path:
        sys.path.insert(0, str(APP_DIR))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)


def setup_django(settings_module: str = DEFAULT_SETTINGS_MODULE) -> None:
    configure_django(settings_module)

    import django

    django.setup()


def ensure_docker_postgres_db(connection) -> None:
    host = str(connection.settings_dict.get("HOST") or "")
    if connection.vendor != "postgresql" or host != "db":
        raise RuntimeError(f"refusing to run outside Docker/Postgres: vendor={connection.vendor} host={host}")
