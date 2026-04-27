from __future__ import annotations

import logging
from collections.abc import Mapping


def _finalize_ingest_status(
    *,
    logger: logging.Logger,
    source_name: str,
    created: int,
    collected_rows: int,
    normalized_rows: int,
    fatal_errors: int,
    fatal_error_message: str,
    extra_quality_metrics: Mapping[str, object] | None = None,
) -> int:
    quality_metrics: dict[str, object] = {
        "created": created,
        "collected_rows": collected_rows,
        "normalized_rows": normalized_rows,
        "fatal_errors": fatal_errors,
    }
    if extra_quality_metrics:
        quality_metrics.update(extra_quality_metrics)
    quality_context = " ".join(f"{key}={value}" for key, value in quality_metrics.items())

    if fatal_errors > 0 and created == 0:
        raise RuntimeError(f"{fatal_error_message}; status=failed {quality_context}")
    if fatal_errors > 0:
        logger.warning("%s ingest quality: status=degraded %s", source_name, quality_context)
        return created

    logger.info("%s ingest quality: status=success %s", source_name, quality_context)
    return created
