"""Funções de runtime Airflow compartilhadas pelas DAGs TRF3/MNI."""


def retry_settings(
    default_attempts: int,
    default_backoff_seconds: float,
    raw_attempts,
    raw_backoff_seconds,
) -> tuple[int, float]:
    attempts = default_attempts if raw_attempts is None else int(raw_attempts)
    backoff = (
        default_backoff_seconds
        if raw_backoff_seconds is None
        else float(raw_backoff_seconds)
    )
    if attempts <= 0:
        raise ValueError('retry_attempts deve ser maior que zero')
    if backoff < 0:
        raise ValueError('retry_backoff_seconds não pode ser negativo')
    return attempts, backoff


def load_config(offline: bool = False, storage_namespace: str | None = None):
    """Carrega Connections apenas durante execução da task, nunca no parse."""
    import json
    import os
    from pathlib import Path

    from airflow.hooks.base import BaseHook
    from src.pgfn_storage import artifact_root, storage_child
    from src.trf3_mni.config import AppConfig

    default_namespace = storage_namespace or 'trf3_mni_initial_petitions'
    default_root = Path('/opt/airflow/data') / default_namespace
    storage_root = artifact_root(default_namespace, default_root)
    pdf_env = 'TRF3_CLUSTER_PDF_DIR' if storage_namespace else 'TRF3_PDF_DIR'
    manifest_env = (
        'TRF3_CLUSTER_MANIFEST_DIR' if storage_namespace else 'TRF3_MANIFEST_DIR'
    )
    pdf_dir = (
        os.environ.get(pdf_env, '').strip()
        or storage_child(storage_root, 'pdfs')
    )
    manifest_dir = (
        os.environ.get(manifest_env, '').strip()
        or storage_child(storage_root, 'manifests')
    )
    if offline:
        return AppConfig(
            report_token='offline',
            report_url='offline://report',
            report_id=421,
            mni_wsdl='offline://mni',
            pje_link='offline://pje',
            mni_login='offline',
            mni_password='offline',
            pdf_dir=pdf_dir,
            manifest_dir=manifest_dir,
            retry_attempts=1,
            retry_backoff_seconds=0,
            initial_document_policy='error',
        )

    report = BaseHook.get_connection('trf3_report')
    mni = BaseHook.get_connection('trf3_mni')
    report_extra = json.loads(report.extra or '{}')
    mni_extra = json.loads(mni.extra or '{}')
    return AppConfig(
        report_token=report.password or '',
        report_url=report.host or '',
        report_id=int(report_extra.get('report_id', 421)),
        mni_wsdl=mni.host or '',
        pje_link=str(mni_extra.get('access_link', '')),
        mni_login=mni.login or '',
        mni_password=mni.password or '',
        pdf_dir=pdf_dir,
        manifest_dir=manifest_dir,
        http_timeout_seconds=float(report_extra.get('timeout_seconds', 30)),
        soap_wsdl_timeout_seconds=float(
            mni_extra.get('wsdl_timeout_seconds', 30)
        ),
        soap_operation_timeout_seconds=float(
            mni_extra.get('operation_timeout_seconds', 60)
        ),
        retry_attempts=int(mni_extra.get('retry_attempts', 3)),
        retry_backoff_seconds=float(
            mni_extra.get('retry_backoff_seconds', 2)
        ),
        initial_document_policy=str(
            mni_extra.get('initial_document_policy', 'error')
        ),
    )


def process_one(process_number: str, storage_namespace: str | None = None) -> dict:
    try:
        from airflow.sdk import get_current_context
    except ImportError:  # Airflow 2 no cluster corporativo
        from airflow.operators.python import get_current_context

    from src.trf3_mni.clients import LocationClient, MniClient
    from src.trf3_mni.pipeline import BatchRunner
    from src.trf3_mni.retry import RetryPolicy, call_with_retry
    from src.trf3_mni.storage import PdfStore

    params = get_current_context()['params']
    offline = bool(params.get('offline', False))
    config = load_config(
        offline=offline,
        storage_namespace=storage_namespace,
    )
    attempts, backoff = retry_settings(
        config.retry_attempts,
        config.retry_backoff_seconds,
        params.get('retry_attempts'),
        params.get('retry_backoff_seconds'),
    )
    policy = RetryPolicy(attempts, backoff)
    if offline:
        from src.trf3_mni.offline import OfflineLocationClient, OfflineMniClient

        mni = OfflineMniClient()
        location = OfflineLocationClient()
    else:
        mni = call_with_retry(
            lambda: MniClient.connect(
                config.mni_wsdl,
                config.mni_login,
                config.mni_password,
                config.soap_wsdl_timeout_seconds,
                config.soap_operation_timeout_seconds,
            ),
            policy,
        )
        location = LocationClient(config.http_timeout_seconds)
    runner = BatchRunner(
        report_client=None,
        mni_client=mni,
        location_client=location,
        pdf_store=PdfStore(config.pdf_dir),
        manifest_store=None,
        retry_policy=policy,
        initial_document_policy=config.initial_document_policy,
        access_link=config.pje_link,
    )
    return runner.process_one(process_number).to_dict()
