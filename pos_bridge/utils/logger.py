from __future__ import annotations

import logging
from pathlib import Path

from pos_bridge.config import load_point_bridge_settings


def get_pos_bridge_logger(name: str = "pos_bridge", *, logfile: Path | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logfile is not None:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        already_configured = any(
            isinstance(handler, logging.FileHandler) and Path(getattr(handler, "baseFilename", "")) == logfile
            for handler in logger.handlers
        )
        if not already_configured:
            handler = logging.FileHandler(logfile, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logger.addHandler(handler)

    return logger


def get_job_logger(job_id: int) -> logging.Logger:
    settings = load_point_bridge_settings()
    logfile = settings.logs_dir / f"sync_job_{job_id}.log"
    return get_pos_bridge_logger(f"pos_bridge.job.{job_id}", logfile=logfile)
