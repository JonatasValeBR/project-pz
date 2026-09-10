"""DAG que gera somente a lista diária de processos do TRF1."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="trf1_process_list",
    description="Scraping TRF1 para gerar a lista de processos, sem baixar PDFs",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "PGFN", "retries": 0},
    params={"date": None},
    tags=["trf1", "scraping", "lista-processos", "pgfn"],
)
def trf1_process_list():
    @task(pool="trf1_pje_pool")
    def scrape_processes() -> dict:
        from datetime import date
        from src.trf1_scraping.airflow_runtime import run_scraping

        context = get_current_context()
        raw_date = context["params"].get("date")
        if raw_date:
            report_date = date.fromisoformat(str(raw_date))
        else:
            logical = context["logical_date"].in_timezone("America/Sao_Paulo")
            report_date = (logical - timedelta(days=1)).date()
        return run_scraping(report_date, context["run_id"])

    scrape_processes()


trf1_process_list()

