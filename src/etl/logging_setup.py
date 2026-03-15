from __future__ import annotations

import logging, logging.config, yaml
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent

def setup_logging() -> None:
    path = _ROOT / "config" / "logging.yaml"
    with path.open() as f:
        cfg = yaml.safe_load(f)
    logging.config.dictConfig(cfg)