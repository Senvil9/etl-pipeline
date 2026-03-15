from __future__ import annotations

import os, yaml
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Project root: 3 levels up from src/elt/config.py
_ROOT = Path(__file__).parent.parent.parent

def load_settings() -> dict:
    cfg_path = _ROOT / "config"/ "settings.yaml"
    with cfg_path.open() as f:
        return yaml.safe_load(f)   
    

def env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise ValueError(
            f"Required environment variable '{key}' is not set. Check your .env file."
        )
    return val