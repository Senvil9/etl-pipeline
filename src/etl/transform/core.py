from __future__ import annotations
import logging
from typing import Iterable
from pydantic import ValidationError
from etl.models import User

log = logging.getLogger(__name__)


def normalize_users(
        records: Iterable[dict],
        source_label: str = "unknown",) -> tuple[list[dict], int]:
    """
    Validate and normalize raw records into the canonical User model.

    Returns:
        (valid_rows, rejected_count)
        valid_rows  - list of dicts ready for loading
        rejected_count - number of records dropped during validation
    """
    valid: list[dict] = []
    rejected = 0

    for raw in records:
        # --- pre-validation: drop rows with missing ID ---
        user_id = raw.get("id") or raw.get("user_id")
        if not user_id:
            log.warning(
                '{"event":"rejected","source":"%s","reason":"missing_id","raw":"%s"}',
                source_label,
                str(raw)[:120],
            )
            rejected += 1
            continue

        # Normalize field name: API uses 'id', db/csv use 'user_id'
        normalised = {
            "user_id":  int(user_id),
            "email":    raw.get("email") or "",
            "first_name": raw.get("first_name") or "",
            "last_name": raw.get("last_name") or "",
            "avater": raw.get("avater"),
        }

        try:
            user = User(**normalised)
            valid.append(user.model_dump())
        except ValidationError as exc:
            log.warning(
                '{"event":"rejected","source":"%s","user_id":%s,"errors":%s}',
                source_label,
                user_id,
                exc.error_count(),
            )
            rejected += 1

    return valid, rejected