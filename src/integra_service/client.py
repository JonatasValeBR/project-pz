from __future__ import annotations

import base64
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from .config import IntegraConfig


class IntegraClient:
    def __init__(self, config: IntegraConfig, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.logger = logger or logging.getLogger("integra_service")
        self.session = requests.Session()

    def _headers(self, token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    def _json_headers(self, token: str) -> Dict[str, str]:
        headers = self._headers(token)
        headers["Content-Type"] = "application/json"
        return headers

    def get_token(self) -> str:
        if not self.config.client_id or not self.config.client_secret:
            raise RuntimeError("Credenciais Integra não configuradas")
        response = self.session.post(
            self.config.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            },
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("Token de acesso não retornado pelo Keycloak")
        return token

    def submit_dados_basicos(self, token: str, link_tribunal: str, numero_processo: str) -> int:
        response = self.session.post(
            self.config.dados_solicitacao_url,
            headers=self._json_headers(token),
            json={
                "linkTribunal": link_tribunal,
                "numeroProcesso": numero_processo,
                "incluirDocumentos": True,
                "movimentos": True,
                "incluirCabecalho": True,
            },
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            value = data.get("idSolicitacao") or data.get("id") or data.get("value")
        else:
            value = data
        return int(str(value))

    def poll_status(self, token: str, job_id: int, url_template: str) -> Dict[str, Any]:
        deadline = time.time() + self.config.polling_timeout
        last_state = ""
        while time.time() < deadline:
            response = self.session.get(
                url_template.format(job_id=job_id),
                headers=self._headers(token),
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            state = str(payload.get("estado") or "").upper()
            if state != last_state:
                self.logger.info("Status recebido: %s", state)
                last_state = state
            if state == "RETORNADA_TRF":
                return payload
            if state.startswith("ERRO_"):
                raise RuntimeError(f"Estado de erro no polling: {state}")
            time.sleep(self.config.polling_interval)
        raise TimeoutError(f"Timeout aguardando conclusão do job {job_id}")

    def get_dados_retorno(self, token: str, job_id: int) -> Dict[str, Any]:
        response = self.session.get(
            self.config.dados_retorno_url.format(job_id=job_id),
            headers=self._headers(token),
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            if not payload:
                raise RuntimeError("Retorno de dados básicos vazio")
            return payload[0]
        return payload

    def submit_pecas(self, token: str, link_tribunal: str, numero_processo: str, id_documento: str) -> int:
        response = self.session.post(
            self.config.pecas_solicitacao_url,
            headers=self._json_headers(token),
            json={
                "linkTribunal": link_tribunal,
                "numeroProcesso": numero_processo,
                "idDocumento": id_documento,
            },
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            value = data.get("idSolicitacao") or data.get("id") or data.get("value")
        else:
            value = data
        return int(str(value))

    def get_pecas_retorno(self, token: str, job_id: int) -> Dict[str, Any]:
        response = self.session.get(
            self.config.pecas_retorno_url.format(job_id=job_id),
            headers=self._headers(token),
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            if not payload:
                raise RuntimeError("Retorno de peças vazio")
            return payload[0]
        return payload

    def download_ceph(
        self,
        token: str,
        bucket: str,
        object_name: str,
        out_path: Optional[Path] = None,
    ) -> bytes:
        response = self.session.get(
            self.config.ceph_download_url,
            headers=self._headers(token),
            params={"nomeBucket": bucket, "nomeObjeto": object_name},
            timeout=self.config.ceph_timeout_seconds,
        )
        response.raise_for_status()
        content_type = (response.headers.get("Content-Type") or "").lower()
        content = response.content
        if "application/json" in content_type:
            try:
                payload = response.json()
                artifact = payload.get("arquivo")
                if isinstance(artifact, str):
                    content = base64.b64decode(artifact)
            except Exception:
                self.logger.warning("Falha ao decodificar payload JSON do CEPH")
        if out_path is not None:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(content)
        return content

    def extract_document_id(self, payload: Dict[str, Any]) -> str:
        documents = payload.get("documento") or payload.get("documentos") or payload.get("documentosProcesso") or []
        if not isinstance(documents, list):
            raise RuntimeError("Payload sem lista de documentos")
        target = self.config.target_document_type.strip().lower()
        for item in documents:
            if not isinstance(item, dict):
                continue
            description = str(item.get("descricao") or "").strip().lower()
            if description != target:
                continue
            for key in ("idDocumento", "id_documento", "id"):
                value = item.get(key)
                if value is not None:
                    return str(value)
        raise RuntimeError(f"Documento alvo não encontrado para '{self.config.target_document_type}'")

    @staticmethod
    def sha256_bytes(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()
