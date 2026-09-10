from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from src.pgfn_storage import artifact_root, storage_child

from dotenv import dotenv_values


def _load_dotenv() -> None:
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / '.env'
    if not env_path.exists():
        return
    for key, value in dotenv_values(env_path).items():
        if value is None:
            continue
        os.environ.setdefault(key, str(value))


_load_dotenv()


def _str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def _int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_path(value: str, fallback: Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    base_dir = Path('/opt/airflow') if Path('/opt/airflow').exists() else Path(__file__).resolve().parents[2]
    return base_dir / candidate


def _resolve_location(value: str, fallback: Path) -> Union[Path, str]:
    if "://" in value:
        return value
    return _resolve_path(value, fallback)


@dataclass(frozen=True)
class IntegraConfig:
    client_id: str
    client_secret: str
    ambiente: str
    service_base_url: str
    keycloak_base_url: str
    realm: str
    token_url: str
    dados_solicitacao_url: str
    dados_status_url: str
    dados_retorno_url: str
    pecas_solicitacao_url: str
    pecas_status_url: str
    pecas_retorno_url: str
    ceph_download_url: str
    json_path: Path
    downloads_dir: Union[Path, str]
    runs_dir: Union[Path, str]
    target_document_type: str
    polling_interval: int
    polling_timeout: int
    timeout_seconds: int
    ceph_timeout_seconds: int
    max_workers: int
    log_level: str
    log_stdout: bool


def load_config() -> IntegraConfig:
    project_root = Path(__file__).resolve().parents[2]
    ambiente = _str("INTEGRA_AMBIENTE", "hom").lower()

    if ambiente == "prod":
        keycloak_base_url = _str(
            "INTEGRA_KC_BASE_URL",
            "https://keycloak-integraservice.pgfn.gov.br",
        )
        service_base_url = _str(
            "INTEGRA_SERVICE_BASE_URL",
            "https://integraservice.pgfn.gov.br",
        )
    else:
        keycloak_base_url = _str(
            "INTEGRA_KC_BASE_URL",
            "https://hom-keycloak-integraservice.ni.estaleiro.serpro.gov.br",
        )
        service_base_url = _str(
            "INTEGRA_SERVICE_BASE_URL",
            "https://hom-integraservice.ni.estaleiro.serpro.gov.br",
        )

    realm = _str("INTEGRA_KC_REALM", "integra-realm")
    token_url = f"{keycloak_base_url}/realms/{realm}/protocol/openid-connect/token"

    base_service = service_base_url.rstrip("/")
    dados_solicitacao_url = f"{base_service}/consulta-processual/api/v1/dados-basicos/solicitacao"
    dados_status_url = f"{base_service}/consulta-processual/api/v1/dados-basicos/solicitacao/{{job_id}}"
    dados_retorno_url = f"{base_service}/consulta-processual/api/v1/dados-basicos/solicitacao/retorno/{{job_id}}"
    pecas_solicitacao_url = f"{base_service}/consulta-processual/api/v1/pecas/solicitacao"
    pecas_status_url = f"{base_service}/consulta-processual/api/v1/pecas/solicitacao/{{job_id}}"
    pecas_retorno_url = f"{base_service}/consulta-processual/api/v1/pecas/solicitacao/retorno/{{job_id}}"
    ceph_download_url = f"{base_service}/ceph/api/v1/objeto/download"

    base_dir = Path('/opt/airflow') if Path('/opt/airflow').exists() else project_root
    default_json_path = base_dir / "data" / "integra_service" / "processos.json"
    storage_namespace = _str("INTEGRA_STORAGE_NAMESPACE", "integra_service")
    storage_root = artifact_root(
        storage_namespace,
        base_dir / "data" / "integra_service",
    )
    default_downloads_dir = storage_child(storage_root, "downloads")
    default_runs_dir = storage_child(storage_root, "runs")

    return IntegraConfig(
        client_id=_str("INTEGRA_CLIENT_ID", ""),
        client_secret=_str("INTEGRA_CLIENT_SECRET", ""),
        ambiente=ambiente,
        service_base_url=service_base_url,
        keycloak_base_url=keycloak_base_url,
        realm=realm,
        token_url=token_url,
        dados_solicitacao_url=dados_solicitacao_url,
        dados_status_url=dados_status_url,
        dados_retorno_url=dados_retorno_url,
        pecas_solicitacao_url=pecas_solicitacao_url,
        pecas_status_url=pecas_status_url,
        pecas_retorno_url=pecas_retorno_url,
        ceph_download_url=ceph_download_url,
        json_path=_resolve_path(_str("INTEGRA_JSON_PATH", str(default_json_path)), default_json_path),
        downloads_dir=(
            _resolve_location(_str("INTEGRA_DOWNLOADS_DIR"), Path("downloads"))
            if _str("INTEGRA_DOWNLOADS_DIR")
            else default_downloads_dir
        ),
        runs_dir=(
            _resolve_location(_str("INTEGRA_RUNS_DIR"), Path("runs"))
            if _str("INTEGRA_RUNS_DIR")
            else default_runs_dir
        ),
        target_document_type=_str("INTEGRA_TIPO_DOCUMENTO_ALVO", "PETIÇÃO INICIAL"),
        polling_interval=_int("INTEGRA_POLLING_INTERVAL", 20),
        polling_timeout=_int("INTEGRA_POLLING_TIMEOUT", 900),
        timeout_seconds=_int("INTEGRA_TIMEOUT_PADRAO", 60),
        ceph_timeout_seconds=_int("INTEGRA_TIMEOUT_CEPH", 180),
        max_workers=_int("INTEGRA_MAX_WORKERS", 4),
        log_level=_str("INTEGRA_LOG_LEVEL", "INFO").upper(),
        log_stdout=_bool("INTEGRA_LOG_STDOUT", True),
    )
