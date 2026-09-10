from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from src.pgfn_storage import ObjectStorage
from .client import IntegraClient
from .config import IntegraConfig


def process_process(process_entry: Dict[str, Any], config: IntegraConfig, run_id: str) -> Dict[str, Any]:
    logger = logging.getLogger("integra_service.workflow")
    link_tribunal = str(process_entry.get("link_tribunal") or process_entry.get("linkTribunal") or "STF")
    numero_processo = str(process_entry.get("numero_processo") or process_entry.get("numeroProcesso") or "")
    if not numero_processo:
        raise ValueError("Entrada de processo sem número")

    storage = ObjectStorage(config.downloads_dir)
    base_key = f"{link_tribunal}/{numero_processo}"

    client = IntegraClient(config, logger=logger)
    token = client.get_token()

    dados_job_id = client.submit_dados_basicos(token, link_tribunal, numero_processo)
    logger.info("Job dados básicos criado para %s: %s", numero_processo, dados_job_id)
    client.poll_status(token, dados_job_id, config.dados_status_url)
    dados_retorno = client.get_dados_retorno(token, dados_job_id)
    bucket = dados_retorno.get("bucketSolicitacao")
    ident = dados_retorno.get("identificadorCeph")
    if not bucket or not ident:
        raise RuntimeError(f"Retorno de dados sem bucket/ident para {numero_processo}")

    content = client.download_ceph(token, bucket, ident)
    full_payload_path = storage.write_bytes(
        f"{base_key}/dadosBasicos_full.json", content
    )
    payload_full = json.loads(content.decode("utf-8"))

    dados_basicos = payload_full.get("dadosBasicos") or payload_full.get("dados_basicos") or payload_full
    dados_basicos_bytes = json.dumps(dados_basicos, ensure_ascii=False, indent=2).encode("utf-8")
    dados_basicos_path = storage.write_bytes(
        f"{base_key}/dadosBasicos.json", dados_basicos_bytes
    )

    document_id = client.extract_document_id(payload_full)
    pecas_job_id = client.submit_pecas(token, link_tribunal, numero_processo, document_id)
    logger.info("Job peças criado para %s: %s", numero_processo, pecas_job_id)
    client.poll_status(token, pecas_job_id, config.pecas_status_url)
    pecas_retorno = client.get_pecas_retorno(token, pecas_job_id)
    peca = pecas_retorno.get("pecaProcesso") or {}
    peca_bucket = peca.get("bucket") or pecas_retorno.get("bucket") or pecas_retorno.get("bucketSolicitacao")
    peca_object = peca.get("identificadorCeph") or pecas_retorno.get("identificadorCeph")
    if not peca_bucket or not peca_object:
        raise RuntimeError(f"Retorno de peças sem bucket/ident para {numero_processo}")

    pdf_content = client.download_ceph(token, peca_bucket, peca_object)
    pdf_path = storage.write_bytes(
        f"{base_key}/peticao_inicial.pdf", pdf_content
    )

    manifest = {
        "processo": numero_processo,
        "linkTribunal": link_tribunal,
        "run_id": run_id,
        "artifacts": [
            {"name": "dados_basicos", "path": str(dados_basicos_path)},
            {"name": "dados_basicos_full", "path": str(full_payload_path)},
            {"name": "peticao_pdf", "path": str(pdf_path)},
        ],
        "document_id": document_id,
    }
    manifest_path = storage.write_bytes(
        f"{base_key}/manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
    )

    return {
        "process_number": numero_processo,
        "status": "success",
        "artifacts": [
            {"name": "dados_basicos", "path": str(dados_basicos_path)},
            {"name": "dados_basicos_full", "path": str(full_payload_path)},
            {"name": "peticao_pdf", "path": str(pdf_path)},
        ],
        "run_id": run_id,
        "manifest_path": str(manifest_path),
    }
