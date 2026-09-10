import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence

from .clients import LocationClient, MniClient, ReportClient
from .config import AppConfig
from .errors import ApplicationError
from .models import ReportWindow
from .pipeline import BatchRunner
from .retry import RetryPolicy, call_with_retry
from .storage import ManifestStore, PdfStore


def _date_value(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('Use o formato AAAA-MM-DD') from exc


def _parse_window(args: argparse.Namespace) -> ReportWindow:
    if args.date:
        if args.start_date or args.end_date:
            raise ValueError('--date não pode ser combinado com --start-date/--end-date')
        return ReportWindow(args.date, args.date)
    if bool(args.start_date) != bool(args.end_date):
        raise ValueError('--start-date e --end-date devem ser informados juntos')
    if args.start_date and args.end_date:
        return ReportWindow(args.start_date, args.end_date)
    yesterday = datetime.now().astimezone().date() - timedelta(days=1)
    return ReportWindow(yesterday, yesterday)


def build_runner(config: AppConfig) -> BatchRunner:
    retry_policy = RetryPolicy(
        attempts=config.retry_attempts,
        backoff_seconds=config.retry_backoff_seconds,
    )
    mni_client = call_with_retry(
        lambda: MniClient.connect(
            config.mni_wsdl,
            config.mni_login,
            config.mni_password,
            config.soap_wsdl_timeout_seconds,
            config.soap_operation_timeout_seconds,
        ),
        retry_policy,
    )
    return BatchRunner(
        report_client=ReportClient(
            config.report_url,
            config.report_token,
            config.report_id,
            config.http_timeout_seconds,
        ),
        mni_client=mni_client,
        location_client=LocationClient(config.http_timeout_seconds),
        pdf_store=PdfStore(config.pdf_dir),
        manifest_store=ManifestStore(config.manifest_dir),
        retry_policy=retry_policy,
        initial_document_policy=config.initial_document_policy,
        access_link=config.pje_link,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='Baixa petições iniciais do TRF3/MNI')
    parser.add_argument('--date', type=_date_value, help='Data única AAAA-MM-DD')
    parser.add_argument('--start-date', type=_date_value, help='Início AAAA-MM-DD')
    parser.add_argument('--end-date', type=_date_value, help='Fim AAAA-MM-DD')
    parser.add_argument('--env-file', type=Path, default=Path('.env'))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s :: %(levelname)s :: %(name)s :: %(message)s',
    )
    try:
        window = _parse_window(args)
        config = AppConfig.from_env(args.env_file)
        manifest = build_runner(config).run(window)
    except (ApplicationError, ValueError) as exc:
        logging.getLogger(__name__).error('%s', exc)
        return 2
    return 1 if manifest.failed else 0
