from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pendulum
from airflow.sdk import dag, get_current_context, task

from src.integra_service.airflow_runtime import (
    load_processes as _load_processes,
    process_one as _process_one,
)


@dag(
    dag_id='integra_service_downloads',
    description='Processa um lote de processos via Integra Service e grava os downloads',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'PGFN', 'retries': 0},
    params={
        'date': None,
        'processes_file': None,
        'source': None,
        'tribunal': None,
        'max_processes': None,
    },
    tags=['integra', 'pgfn', 'downloads'],
)
def integra_service_downloads():
    @task
    def resolve_window() -> dict:
        context = get_current_context()
        params = context['params']
        single_date = params.get('date')
        if single_date:
            day = datetime.fromisoformat(str(single_date)).date()
        elif context.get('data_interval_start') and context.get('data_interval_end'):
            interval_start = context['data_interval_start'].in_timezone('America/Sao_Paulo')
            interval_end = context['data_interval_end'].in_timezone('America/Sao_Paulo')
            day = interval_start.date()
            if interval_end.date() != day:
                day = interval_end.date()
        else:
            day = datetime.now(timezone.utc).date()
        return {
            'date': day.isoformat(),
            'started_at': datetime.now(timezone.utc).isoformat(),
        }

    @task
    def load_processes(window: dict) -> list[dict]:
        params = get_current_context()['params']
        source = params.get('source')
        tribunal = params.get('tribunal')
        processes = _load_processes(source=source, tribunal=tribunal)
        if not processes:
            raise ValueError('Nenhum processo encontrado na fonte configurada')
        limit = params.get('max_processes')
        if limit is not None:
            limit = int(limit)
            if limit <= 0:
                raise ValueError('max_processes deve ser maior que zero')
            processes = processes[:limit]
        return processes

    @task
    def process_processes(processes: list[dict]) -> list[dict]:
        context = get_current_context()
        run_id = str(context['run_id'])
        results = []
        for process_entry in processes:
            results.append(_process_one(process_entry, run_id=run_id))
        return results

    @task
    def publish_manifest(window: dict, results: list[dict]) -> dict:
        from src.pgfn_storage import ObjectStorage, artifact_root, storage_child

        run_id = get_current_context()['run_id']
        manifest_path = Path(get_current_context().get('conf').get('core').get('dags_folder', '/opt/airflow/dags')) if False else None
        payload = {
            'window': window,
            'run_id': run_id,
            'results': results,
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        storage = ObjectStorage(storage_child(
            artifact_root('integra_service', '/opt/airflow/data/integra_service'),
            'runs',
        ))
        output_path = storage.write_bytes(
            f'{run_id}.json',
            __import__('json').dumps(
                payload, ensure_ascii=False, indent=2
            ).encode('utf-8'),
        )
        return {'path': str(output_path), 'total': len(results)}

    @task
    def quality_gate(summary: dict) -> None:
        if summary.get('total', 0) == 0:
            raise ValueError('Nenhum processo foi processado')

    window = resolve_window()
    processes = load_processes(window)
    results = process_processes(processes)
    summary = publish_manifest(window, results)
    quality_gate(summary)


integra_service_downloads()
