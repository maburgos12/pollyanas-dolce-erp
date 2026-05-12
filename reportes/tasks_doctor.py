from __future__ import annotations

import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name="reportes.erp_doctor_daily_report")
def erp_doctor_daily_report() -> dict:
    from scripts.erp_doctor import run_doctor

    report = run_doctor(quick=True, full=False, fix=True, email=True)
    logger.info(
        "ERP Doctor diario finalizado status=%s email_sent=%s",
        report.get("status"),
        report.get("email_sent"),
    )
    return {
        "status": report.get("status"),
        "email_sent": report.get("email_sent", False),
        "checks": [
            {
                "name": check.get("name"),
                "status": check.get("status"),
                "summary": check.get("summary"),
                "fixed": check.get("fixed", False),
                "fix_action": check.get("fix_action"),
            }
            for check in report.get("checks", [])
            if check.get("status") in {"WARN", "FAIL"} or check.get("fixed")
        ],
    }
