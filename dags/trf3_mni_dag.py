"""DAG de download de petições iniciais do TRF3 via MNI."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from src.trf3_mni.airflow_runtime import (
    load_config as _load_config,
    process_one as _process_one,
    retry_settings as _retry_settings,
)


def _limit_processes(processes: list[str], raw_limit) -> list[str]:
    if raw_limit is None:
        return processes
    limit = int(raw_limit)
    if limit <= 0:
        raise ValueError('max_processes deve ser maior que zero')
    return processes[:limit]


@dag(
    dag_id='trf3_mni_initial_petitions',
    description='Consulta relatório TRF3 e baixa petições iniciais via MNI',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'PGFN', 'retries': 0},
    params={
        'date': None,
        'start_date': None,
        'end_date': None,
        'offline': False,
        'max_processes': None,
        'retry_attempts': None,
        'retry_backoff_seconds': None,
    },
    tags=['trf3', 'mni', 'pgfn'],
)
def trf3_mni_initial_petitions():
    @task
    def resolve_window() -> dict:
        from datetime import date, datetime, timezone

        context = get_current_context()
        params = context['params']
        single_date = params.get('date')
        start_raw = params.get('start_date')
        end_raw = params.get('end_date')
        if single_date:
            start = end = date.fromisoformat(str(single_date))
        elif start_raw and end_raw:
            start = date.fromisoformat(str(start_raw))
            end = date.fromisoformat(str(end_raw))
        elif context.get('data_interval_start') and context.get('data_interval_end'):
            interval_start = context['data_interval_start'].in_timezone(
                'America/Sao_Paulo'
            )
            interval_end = context['data_interval_end'].in_timezone(
                'America/Sao_Paulo'
            )
            start = interval_start.date()
            end = (interval_end - timedelta(microseconds=1)).date()
        else:
            raise ValueError(
                'Informe date ou o par start_date/end_date ao disparar a DAG'
            )
        if end < start:
            raise ValueError('end_date não pode ser anterior a start_date')
        return {
            'start': start.isoformat(),
            'end': end.isoformat(),
            'started_at': datetime.now(timezone.utc).isoformat(),
        }

    @task
    def fetch_processes(window: dict) -> dict:
        from datetime import date
        import os
        from pathlib import Path

        from src.trf3_mni.clients import ReportClient
        from src.trf3_mni.models import ReportWindow
        from src.trf3_mni.pipeline import normalize_process_numbers
        from src.trf3_mni.retry import RetryPolicy, call_with_retry
        from src.trf3_mni.storage import ReportStore
        from src.pgfn_storage import artifact_root, storage_child

        params = get_current_context()['params']
        offline = bool(params.get('offline', False))
        config = _load_config(offline=offline)
        report_window = ReportWindow(
            date.fromisoformat(window['start']), date.fromisoformat(window['end'])
        )
        attempts, backoff = _retry_settings(
            config.retry_attempts,
            config.retry_backoff_seconds,
            params.get('retry_attempts'),
            params.get('retry_backoff_seconds'),
        )
        policy = RetryPolicy(attempts, backoff)
        if offline:
            from src.trf3_mni.offline import OfflineReportClient

            client = OfflineReportClient()
        else:
            client = ReportClient(
                config.report_url,
                config.report_token,
                config.report_id,
                config.http_timeout_seconds,
            )
        report = call_with_retry(lambda: client.fetch(report_window), policy)
        processes = normalize_process_numbers(report)
        report_root = artifact_root(
            'trf3_mni_initial_petitions',
            '/opt/airflow/data/trf3_mni_initial_petitions',
        )
        report_dir = (
            os.environ.get('TRF3_REPORT_DIR', '').strip()
            or storage_child(report_root, 'relatorios')
        )
        report_artifacts = ReportStore(report_dir).save(report, report_window)
        raw_limit = params.get('max_processes')
        return {
            'processes': _limit_processes(processes, raw_limit),
            'report_artifacts': [item.to_dict() for item in report_artifacts],
        }

    @task
    def select_processes(report_data: dict) -> list[str]:
        return report_data['processes']

    @task(pool='mni_pool')
    def process_one(process_number: str) -> dict:
        return _process_one(process_number)

    @task
    def publish_manifest(
        window: dict,
        report_artifacts: list[dict],
        results: list[dict],
    ) -> dict:
        from datetime import date, datetime, timezone
        import os
        from pathlib import Path

        from src.trf3_mni.models import (
            BatchManifest,
            ProcessResult,
            ReportWindow,
            StoredArtifact,
        )
        from src.trf3_mni.storage import FailureReportStore, ManifestStore
        from src.pgfn_storage import artifact_root, storage_child

        def artifact_location(value: str):
            return value if "://" in value else Path(value)

        process_results = []
        for result in results:
            artifacts = [
                StoredArtifact(
                    path=artifact_location(item['path']),
                    sha256=item['sha256'],
                    size_bytes=item['size_bytes'],
                    reused=item['reused'],
                )
                for item in result.get('artifacts', [])
            ]
            process_results.append(ProcessResult(
                process_number=result['process_number'],
                status=result['status'],
                artifacts=artifacts,
                error_type=result.get('error_type'),
                error_message=result.get('error_message'),
                started_at=(
                    datetime.fromisoformat(result['started_at'])
                    if result.get('started_at') else None
                ),
                finished_at=(
                    datetime.fromisoformat(result['finished_at'])
                    if result.get('finished_at') else None
                ),
                duration_seconds=result.get('duration_seconds'),
            ))
        context = get_current_context()
        run_id = context['run_id']
        manifest = BatchManifest(
            window=ReportWindow(
                date.fromisoformat(window['start']), date.fromisoformat(window['end'])
            ),
            started_at=datetime.fromisoformat(window['started_at']),
            finished_at=datetime.now(timezone.utc),
            results=process_results,
            report_artifacts=[
                StoredArtifact(
                    path=artifact_location(item['path']),
                    sha256=item['sha256'],
                    size_bytes=item['size_bytes'],
                    reused=item['reused'],
                )
                for item in report_artifacts
            ],
            run_id=run_id,
        )
        offline = bool(context['params'].get('offline', False))
        config = _load_config(offline=offline)
        path = ManifestStore(config.manifest_dir).save(manifest, run_id=run_id)
        report_root = artifact_root(
            'trf3_mni_initial_petitions',
            '/opt/airflow/data/trf3_mni_initial_petitions',
        )
        report_dir = (
            os.environ.get('TRF3_REPORT_DIR', '').strip()
            or storage_child(report_root, 'relatorios')
        )
        failure_artifacts = FailureReportStore(report_dir).save(
            manifest.to_dict(), run_id=run_id
        )
        return {
            'path': str(path),
            'total': len(results),
            'succeeded': manifest.succeeded,
            'failed': manifest.failed,
            'failure_artifacts': [
                item.to_dict() for item in failure_artifacts
            ],
        }

    @task
    def quality_gate(summary: dict) -> None:
        if summary['failed']:
            raise ValueError(
                f"Lote terminou com {summary['failed']} falha(s); "
                f"consulte {summary['path']}"
            )

    window = resolve_window()
    report_data = fetch_processes(window)
    processes = select_processes(report_data)
    results = process_one.expand(process_number=processes)
    summary = publish_manifest(
        window,
        report_data['report_artifacts'],
        results,
    )
    quality_gate(summary)


trf3_mni_initial_petitions()
