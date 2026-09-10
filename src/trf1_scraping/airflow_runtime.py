import json
import re
from datetime import date
from pathlib import Path

from .config import Trf1Config
from .scraper import Trf1ProcessListScraper
from .storage import save_process_list
from src.pgfn_storage import ObjectStorage, artifact_root, storage_child


def load_config() -> Trf1Config:
    import os
    from airflow.hooks.base import BaseHook
    from src.pgfn_storage import artifact_root, storage_child

    connection = BaseHook.get_connection("trf1_pje")
    extra = json.loads(connection.extra or "{}")
    storage_root = artifact_root(
        "trf1_process_list",
        "/opt/airflow/data/trf1_process_list",
    )
    config = Trf1Config(
        endpoint=connection.host or "",
        login=connection.login or "",
        password=connection.password or "",
        totp_secret=str(extra.get("totp_secret", "")),
        reports_url=str(extra.get(
            "reports_url",
            "https://pje1g.trf1.jus.br/pje/Processo/ConsultaProcesso/listView.seam",
        )),
        timeout_seconds=int(extra.get("timeout_seconds", 60)),
        headless=bool(extra.get("headless", True)),
        report_dir=(
            os.environ.get("TRF1_REPORT_DIR", "").strip()
            or storage_child(storage_root, "relatorios")
        ),
        screenshot_dir=Path(os.environ.get(
            "TRF1_SCREENSHOT_DIR",
            "/opt/airflow/data/trf1_process_list/screenshots",
        )),
    )
    config.validate()
    return config


def run_scraping(report_date: date, run_id: str) -> dict:
    config = load_config()
    scraper = Trf1ProcessListScraper(config)
    try:
        records = scraper.run(report_date)
        return save_process_list(records, report_date, config.report_dir)
    except Exception:
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)[:120]
        storage_root = artifact_root(
            "trf1_process_list",
            "/opt/airflow/data/trf1_process_list",
        )
        ObjectStorage(storage_child(storage_root, "screenshots")).write_bytes(
            f"falha-{report_date.isoformat()}-{safe_run_id}.png",
            scraper.screenshot_bytes(),
        )
        raise
    finally:
        scraper.close()
