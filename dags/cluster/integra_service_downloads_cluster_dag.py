"""Variante do Integra Service para o cluster Airflow da PGFN."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context


def _connection_environment(connection_id: str = "integra_service") -> dict[str, str]:
    """Traduz uma Connection do Airflow sem persistir ou registrar segredos."""
    connection = BaseHook.get_connection(connection_id)
    extra = connection.extra_dejson
    values = {
        "INTEGRA_CLIENT_ID": connection.login or "",
        "INTEGRA_CLIENT_SECRET": connection.password or "",
        "INTEGRA_SERVICE_BASE_URL": connection.host or "",
        "INTEGRA_AMBIENTE": str(extra.get("ambiente", "hom")),
        "INTEGRA_KC_BASE_URL": str(extra.get("keycloak_base_url", "")),
        "INTEGRA_KC_REALM": str(extra.get("realm", "integra-realm")),
        "INTEGRA_STORAGE_NAMESPACE": "integra_service_downloads_cluster",
    }
    if extra.get("downloads_dir"):
        values["INTEGRA_DOWNLOADS_DIR"] = str(extra["downloads_dir"])
    if extra.get("runs_dir"):
        values["INTEGRA_RUNS_DIR"] = str(extra["runs_dir"])
    missing = [key for key, value in values.items() if not value.strip()]
    if missing:
        raise RuntimeError(
            f"Connection {connection_id} incompleta: " + ", ".join(missing)
        )
    return values


@dag(
    dag_id="integra_service_downloads_cluster",
    description="Processa lote via Integra Service no cluster PGFN",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "PGFN/UnB", "retries": 0},
    params={
        "date": None,
        "source": None,
        "tribunal": None,
        "max_processes": None,
        "connection_id": "integra_service",
    },
    tags=["unb-pgfn", "unb", "pgfn", "integra", "downloads"],
)
def integra_service_downloads_cluster():
    @task
    def resolve_window() -> dict:
        context = get_current_context()
        raw_date = context["params"].get("date")
        day = (
            datetime.fromisoformat(str(raw_date)).date()
            if raw_date
            else datetime.now(timezone.utc).date()
        )
        return {
            "date": day.isoformat(),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

    @task
    def load_processes() -> list[dict]:
        from src.integra_service.airflow_runtime import load_processes as load

        params = get_current_context()["params"]
        processes = load(
            source=params.get("source"),
            tribunal=params.get("tribunal"),
        )
        if not processes:
            raise ValueError("Nenhum processo encontrado na fonte configurada")
        raw_limit = params.get("max_processes")
        if raw_limit is not None:
            limit = int(raw_limit)
            if limit <= 0:
                raise ValueError("max_processes deve ser maior que zero")
            processes = processes[:limit]
        return processes

    @task
    def process_processes(processes: list[dict]) -> list[dict]:
        from src.integra_service.workflow import process_process
        from src.integra_service.config import load_config

        context = get_current_context()
        connection_id = str(context["params"].get("connection_id") or "integra_service")
        injected = _connection_environment(connection_id)
        previous = {key: os.environ.get(key) for key in injected}
        os.environ.update(injected)
        try:
            config = load_config()
            run_id = str(context["run_id"])
            return [process_process(item, config, run_id) for item in processes]
        finally:
            for key, old_value in previous.items():
                if old_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old_value

    @task
    def publish_manifest(window: dict, results: list[dict]) -> dict:
        from src.pgfn_storage import ObjectStorage, artifact_root, storage_child

        run_id = str(get_current_context()["run_id"])
        storage = ObjectStorage(
            storage_child(
                artifact_root(
                    "integra_service_downloads_cluster",
                    "/opt/airflow/data/integra_service_downloads_cluster",
                ),
                "runs",
            )
        )
        safe_run_id = "".join(
            char if char.isalnum() or char in "._-" else "_" for char in run_id
        )[:180]
        payload = {
            "window": window,
            "run_id": run_id,
            "results": results,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        output_path = storage.write_bytes(
            f"{safe_run_id}.json",
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        )
        return {"path": str(output_path), "total": len(results)}

    @task
    def quality_gate(summary: dict) -> None:
        if summary.get("total", 0) == 0:
            raise ValueError("Nenhum processo foi processado")

    window = resolve_window()
    processes = load_processes()
    results = process_processes(processes)
    summary = publish_manifest(window, results)
    quality_gate(summary)


dag_final = integra_service_downloads_cluster()
