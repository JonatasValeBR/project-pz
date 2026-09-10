import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional, Union

from dotenv import dotenv_values

from .errors import ConfigurationError


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f'{name} deve ser um número inteiro') from exc
    if value <= 0:
        raise ConfigurationError(f'{name} deve ser maior que zero')
    return value


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f'{name} deve ser um número') from exc
    if value <= 0:
        raise ConfigurationError(f'{name} deve ser maior que zero')
    return value


@dataclass(frozen=True)
class AppConfig:
    report_token: str = field(repr=False)
    report_url: str
    report_id: int
    mni_wsdl: str
    pje_link: str
    mni_login: str = field(repr=False)
    mni_password: str = field(repr=False)
    pdf_dir: Union[Path, str]
    manifest_dir: Union[Path, str]
    http_timeout_seconds: float = 30.0
    soap_wsdl_timeout_seconds: float = 30.0
    soap_operation_timeout_seconds: float = 60.0
    retry_attempts: int = 3
    retry_backoff_seconds: float = 2.0
    initial_document_policy: str = 'error'

    @classmethod
    def from_env(cls, dotenv_path: Optional[Path] = Path('.env')) -> 'AppConfig':
        file_values = dotenv_values(dotenv_path) if dotenv_path else {}
        values = {
            key: str(value)
            for key, value in file_values.items()
            if value is not None
        }
        values.update(os.environ)

        def required(name: str) -> str:
            value = values.get(name, '').strip()
            if not value:
                raise ConfigurationError(f'Configuração obrigatória ausente: {name}')
            return value

        policy = values.get('INITIAL_DOCUMENT_POLICY', 'error').strip().lower()
        if policy not in {'error', 'first', 'all'}:
            raise ConfigurationError(
                'INITIAL_DOCUMENT_POLICY deve ser error, first ou all'
            )

        pdf_dir = Path(required('PATH_PDF')).expanduser()
        manifest_value = values.get('PATH_MANIFEST') or values.get('PATH_RELATORIO')
        manifest_dir = (
            Path(manifest_value).expanduser()
            if manifest_value
            else pdf_dir / 'manifests'
        )

        return cls(
            report_token=required('TOKEN'),
            report_url=values.get(
                'REPORT_URL',
                'https://relatorios-gerenciais-1g.app.trf3.jus.br/Api/DownloadJSON',
            ).strip(),
            report_id=_positive_int(values, 'REPORT_ID', 421),
            mni_wsdl=required('ENDPOINT_TRF3'),
            pje_link=required('LINK_ACESSO_TRF3'),
            mni_login=required('LOGIN'),
            mni_password=required('SENHA'),
            pdf_dir=pdf_dir,
            manifest_dir=manifest_dir,
            http_timeout_seconds=_positive_float(
                values, 'HTTP_TIMEOUT_SECONDS', 30.0
            ),
            soap_wsdl_timeout_seconds=_positive_float(
                values, 'SOAP_WSDL_TIMEOUT_SECONDS', 30.0
            ),
            soap_operation_timeout_seconds=_positive_float(
                values, 'SOAP_OPERATION_TIMEOUT_SECONDS', 60.0
            ),
            retry_attempts=_positive_int(values, 'RETRY_ATTEMPTS', 3),
            retry_backoff_seconds=_positive_float(
                values, 'RETRY_BACKOFF_SECONDS', 2.0
            ),
            initial_document_policy=policy,
        )
