from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


APP_NAME = "airdash"
CONFIG_FILE = "config.json"


@dataclass(frozen=True)
class AirflowConfig:
    url: str
    token: str


def config_path() -> Path:
    if os.name == "nt":
        root = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        root = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return root / APP_NAME / CONFIG_FILE


def load_config() -> AirflowConfig | None:
    path = config_path()
    if not path.exists():
        return None

    try:
        raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    url = str(raw.get("url", "")).strip()
    token = str(raw.get("token", "")).strip()
    if not url or not token:
        return None
    return AirflowConfig(url=url, token=token)


def save_config(config: AirflowConfig) -> Path:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"url": config.url, "token": config.token}, indent=2),
        encoding="utf-8",
    )
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return path


def normalize_airflow_url(url: str) -> str:
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith("/api/v2"):
        return cleaned[: -len("/api/v2")]
    return cleaned
