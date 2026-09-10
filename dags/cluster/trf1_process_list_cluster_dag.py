"""Variante da lista TRF1 para o cluster Kubernetes da PGFN.

Os segredos são renderizados a partir da Connection ``trf1_pje``. A imagem
configurada precisa conter o pacote ``src.trf1_scraping``; a imagem-base do
cluster, isoladamente, contém apenas as ferramentas de scraping.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from kubernetes.client import models as k8s


SCRAPING_IMAGE = os.environ.get(
    "PGFN_SCRAPING_IMAGE",
    "southamerica-east1-docker.pkg.dev/pgfn-cgti/"
    "repo-airflow-custom/airflow-scrapping-base:v6",
)

TOLERATIONS = [
    k8s.V1Toleration(
        key="env",
        operator="Equal",
        value="svc",
        effect="NoSchedule",
    )
]

AFFINITY = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
            node_selector_terms=[
                k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(
                            key="env",
                            operator="In",
                            values=["svc"],
                        )
                    ]
                )
            ]
        )
    )
)

# Estes valores não são segredos no arquivo: o Airflow os resolve em runtime.
# A estratégia definitiva (Connection, Vault ou Kubernetes Secret) deve ser
# confirmada com a equipe do cluster antes do primeiro disparo real.
TRF1_CONNECTION_ENV = {
    "TRF1_ENDPOINT": "{{ conn.trf1_pje.host }}",
    "TRF1_LOGIN": "{{ conn.trf1_pje.login }}",
    "TRF1_PASSWORD": "{{ conn.trf1_pje.password }}",
    "TRF1_TOTP_SECRET": "{{ conn.trf1_pje.extra_dejson.totp_secret }}",
    "TRF1_REPORTS_URL": "{{ conn.trf1_pje.extra_dejson.reports_url }}",
    "TRF1_TIMEOUT_SECONDS": "{{ conn.trf1_pje.extra_dejson.timeout_seconds }}",
    "TRF1_HEADLESS": "{{ conn.trf1_pje.extra_dejson.headless }}",
    "PGFN_ARTIFACT_STORE_URI": "{{ var.value.pgfn_artifact_store_uri }}",
    "TRF1_SCREENSHOT_DIR": "/opt/airflow/data/trf1_process_list_cluster/screenshots",
}


@dag(
    dag_id="trf1_process_list_cluster",
    description="Lista diária TRF1 no perfil Kubernetes de scraping da PGFN",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "PGFN/UnB", "retries": 0},
    params={"date": None},
    tags=["unb-pgfn", "unb", "pgfn", "trf1", "scraping", "kubernetes"],
)
def trf1_process_list_cluster():
    @task
    def resolve_report_date() -> str:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        raw_date = context["params"].get("date")
        if raw_date:
            return str(raw_date)
        logical = context["logical_date"].in_timezone("America/Sao_Paulo")
        return (logical - timedelta(days=1)).date().isoformat()

    @task.kubernetes(
        image=SCRAPING_IMAGE,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        namespace="airflow",
        tolerations=TOLERATIONS,
        affinity=AFFINITY,
        env_vars=TRF1_CONNECTION_ENV,
    )
    def scrape_processes(report_date_raw: str) -> dict:
        import os
        from datetime import date
        from pathlib import Path

        from src.trf1_scraping.config import Trf1Config
        from src.trf1_scraping.scraper import Trf1ProcessListScraper
        from src.trf1_scraping.storage import save_process_list
        from src.pgfn_storage import ObjectStorage, artifact_root, storage_child

        report_date = date.fromisoformat(report_date_raw)

        required = (
            "TRF1_ENDPOINT",
            "TRF1_LOGIN",
            "TRF1_PASSWORD",
            "TRF1_TOTP_SECRET",
            "TRF1_REPORTS_URL",
        )
        missing = [name for name in required if not os.environ.get(name, "").strip()]
        if missing:
            raise RuntimeError(
                "Connection trf1_pje incompleta no cluster: " + ", ".join(missing)
            )

        config = Trf1Config(
            endpoint=os.environ["TRF1_ENDPOINT"],
            login=os.environ["TRF1_LOGIN"],
            password=os.environ["TRF1_PASSWORD"],
            totp_secret=os.environ["TRF1_TOTP_SECRET"],
            reports_url=os.environ["TRF1_REPORTS_URL"],
            timeout_seconds=int(os.environ.get("TRF1_TIMEOUT_SECONDS") or "60"),
            headless=os.environ.get("TRF1_HEADLESS", "true").lower()
            not in {"0", "false", "no"},
            report_dir=storage_child(
                artifact_root(
                    "trf1_process_list_cluster",
                    "/opt/airflow/data/trf1_process_list_cluster",
                ),
                "relatorios",
            ),
            screenshot_dir=Path(os.environ["TRF1_SCREENSHOT_DIR"]),
        )
        config.validate()
        scraper = Trf1ProcessListScraper(config)
        try:
            records = scraper.run(report_date)
            return save_process_list(records, report_date, config.report_dir)
        except Exception:
            ObjectStorage(storage_child(
                artifact_root(
                    "trf1_process_list_cluster",
                    "/opt/airflow/data/trf1_process_list_cluster",
                ),
                "screenshots",
            )).write_bytes(
                f"falha-{report_date.isoformat()}.png",
                scraper.screenshot_bytes(),
            )
            raise
        finally:
            scraper.close()

    scrape_processes(resolve_report_date())


dag_final = trf1_process_list_cluster()
