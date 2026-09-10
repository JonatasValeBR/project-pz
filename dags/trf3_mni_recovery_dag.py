"""DAG manual para reprocessar apenas falhas registradas em um manifesto."""

import json
import re
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task
from src.trf3_mni.airflow_runtime import (
    load_config,
    process_one as process_one_runtime,
)


def _resolve_manifest_path(manifest_dir: Path, raw_name) -> Path:
    name = str(raw_name or '').strip()
    if not name or Path(name).name != name or not name.endswith('.json'):
        raise ValueError('source_manifest deve ser apenas o nome de um arquivo JSON')
    root = manifest_dir.resolve()
    candidate = (root / name).resolve()
    if candidate.parent != root:
        raise ValueError('source_manifest está fora da pasta de manifestos')
    if not candidate.is_file():
        raise ValueError('source_manifest não foi encontrado')
    return candidate


def _normalize_error_types(raw_error_types) -> set[str] | None:
    if raw_error_types is None:
        return None
    if isinstance(raw_error_types, str):
        values = [item.strip() for item in raw_error_types.split(',')]
    elif isinstance(raw_error_types, list):
        values = [str(item).strip() for item in raw_error_types]
    else:
        raise ValueError('error_types deve ser uma lista ou texto separado por vírgula')
    selected = {item for item in values if item}
    if not selected:
        raise ValueError('error_types não pode ser vazio')
    if any(not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', item) for item in selected):
        raise ValueError('error_types contém um tipo inválido')
    return selected


def _failed_processes(
    manifest: dict,
    raw_limit=None,
    raw_error_types=None,
) -> list[str]:
    results = manifest.get('results')
    if not isinstance(results, list):
        raise ValueError('Manifesto de origem não possui uma lista results')
    processes = set()
    selected_error_types = _normalize_error_types(raw_error_types)
    for result in results:
        if not isinstance(result, dict):
            raise ValueError('Manifesto de origem possui resultado inválido')
        if result.get('status') != 'failed':
            continue
        if (
            selected_error_types is not None
            and result.get('error_type') not in selected_error_types
        ):
            continue
        normalized = re.sub(r'\D', '', str(result.get('process_number', '')))
        if not normalized:
            raise ValueError('Falha do manifesto não possui processo válido')
        processes.add(normalized)
    selected = sorted(processes)
    if not selected:
        raise ValueError('Manifesto não possui falhas para os filtros informados')
    if raw_limit is not None:
        limit = int(raw_limit)
        if limit <= 0:
            raise ValueError('max_processes deve ser maior que zero')
        selected = selected[:limit]
    return selected


@dag(
    dag_id='trf3_mni_retry_failures',
    description='Reprocessa somente falhas de um manifesto TRF3/MNI',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'PGFN', 'retries': 0},
    params={
        'source_manifest': None,
        'offline': False,
        'max_processes': None,
        'error_types': None,
        'retry_attempts': None,
        'retry_backoff_seconds': None,
    },
    tags=['trf3', 'mni', 'pgfn', 'recovery'],
)
def trf3_mni_retry_failures():
    @task
    def load_failed_processes() -> dict:
        from datetime import datetime, timezone
        from src.pgfn_storage import ObjectStorage

        params = get_current_context()['params']
        offline = bool(params.get('offline', False))
        config = load_config(offline=offline)
        name = str(params.get('source_manifest') or '').strip()
        if not name or Path(name).name != name or not name.endswith('.json'):
            raise ValueError(
                'source_manifest deve ser apenas o nome de um arquivo JSON'
            )
        storage = ObjectStorage(config.manifest_dir)
        if not storage.exists(name):
            raise ValueError('source_manifest não foi encontrado')
        try:
            manifest = json.loads(storage.read_bytes(name).decode('utf-8'))
        except (OSError, UnicodeError, ValueError) as exc:
            raise ValueError('Não foi possível ler o manifesto de origem') from exc
        window = manifest.get('window')
        if not isinstance(window, dict) or not window.get('start') or not window.get('end'):
            raise ValueError('Manifesto de origem não possui janela válida')
        return {
            'source_manifest': str(storage.location(name)),
            'window': {'start': window['start'], 'end': window['end']},
            'started_at': datetime.now(timezone.utc).isoformat(),
            'processes': _failed_processes(
                manifest,
                params.get('max_processes'),
                params.get('error_types'),
            ),
            'error_types': sorted(
                _normalize_error_types(params.get('error_types')) or []
            ),
            'report_artifacts': manifest.get('report_artifacts', []),
        }

    @task
    def select_processes(recovery: dict) -> list[str]:
        return recovery['processes']

    @task(pool='mni_pool')
    def process_one(process_number: str) -> dict:
        return process_one_runtime(process_number)

    @task
    def publish_recovery_manifest(recovery: dict, results: list[dict]) -> dict:
        from datetime import date, datetime, timezone
        import os

        from src.trf3_mni.models import (
            BatchManifest,
            ProcessResult,
            ReportWindow,
            StoredArtifact,
        )
        from src.trf3_mni.storage import FailureReportStore, ManifestStore
        from src.pgfn_storage import artifact_root, storage_child

        def artifact(item: dict) -> StoredArtifact:
            raw_path = str(item['path'])
            return StoredArtifact(
                path=raw_path if '://' in raw_path else Path(raw_path),
                sha256=item['sha256'],
                size_bytes=item['size_bytes'],
                reused=item['reused'],
            )

        process_results = []
        for result in results:
            process_results.append(ProcessResult(
                process_number=result['process_number'],
                status=result['status'],
                artifacts=[artifact(item) for item in result.get('artifacts', [])],
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
        offline = bool(context['params'].get('offline', False))
        config = load_config(offline=offline)
        run_id = context['run_id']
        manifest = BatchManifest(
            window=ReportWindow(
                date.fromisoformat(recovery['window']['start']),
                date.fromisoformat(recovery['window']['end']),
            ),
            started_at=datetime.fromisoformat(recovery['started_at']),
            finished_at=datetime.now(timezone.utc),
            results=process_results,
            report_artifacts=[
                artifact(item) for item in recovery.get('report_artifacts', [])
            ],
            run_id=run_id,
            source_manifest=recovery['source_manifest'],
        )
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
            'source_manifest': recovery['source_manifest'],
            'failure_artifacts': [
                item.to_dict() for item in failure_artifacts
            ],
        }

    @task
    def quality_gate(summary: dict) -> None:
        if summary['failed']:
            raise ValueError(
                f"Recuperação terminou com {summary['failed']} falha(s); "
                f"consulte {summary['path']}"
            )

    recovery = load_failed_processes()
    processes = select_processes(recovery)
    results = process_one.expand(process_number=processes)
    summary = publish_recovery_manifest(recovery, results)
    quality_gate(summary)


trf3_mni_retry_failures()
