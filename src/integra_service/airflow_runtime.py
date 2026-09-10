from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from airflow.sdk import get_current_context
except ImportError:  # Airflow 2 no cluster corporativo
    from airflow.operators.python import get_current_context

from .config import IntegraConfig, load_config as _load_config
from .workflow import process_process


def load_config() -> IntegraConfig:
    return _load_config()


def _iter_process_sources() -> List[Dict[str, Any]]:
    project_root = Path(__file__).resolve().parents[2]
    return [
        {
            'name': 'json-explicit',
            'path': None,
            'pattern': None,
            'tribunal': None,
            'mapper': 'json',
        },
        {
            'name': 'trf1-relatorios',
            'path': Path('/opt/airflow/data/trf1_process_list/relatorios'),
            'pattern': 'lista-processos-*.json',
            'tribunal': 'TRF1',
            'mapper': 'trf1',
        },
        {
            'name': 'trf1-cluster-relatorios',
            'path': Path('/opt/airflow/data/trf1_process_list_cluster/relatorios'),
            'pattern': 'lista-processos-*.json',
            'tribunal': 'TRF1',
            'mapper': 'trf1',
        },
        {
            'name': 'trf1-relatorios-local',
            'path': project_root / 'data' / 'trf1_process_list' / 'relatorios',
            'pattern': 'lista-processos-*.json',
            'tribunal': 'TRF1',
            'mapper': 'trf1',
        },
    ]


def _normalize_process_entry(item: Dict[str, Any], tribunal: Optional[str] = None) -> Optional[Dict[str, Any]]:
    numero = str(item.get('nr_processo') or item.get('numero_processo') or item.get('numero') or '').strip()
    if not numero:
        return None
    payload = {
        'numero_processo': numero,
        'link_tribunal': tribunal or str(item.get('link_tribunal') or item.get('tribunal') or 'TRF1'),
    }
    for key in ('classe', 'secao', 'data', 'id_processo'):
        if key in item:
            payload[key] = item[key]
    return payload


def _load_from_json(path: Path) -> List[Dict[str, Any]]:
    with path.open('r', encoding='utf-8') as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        processos = payload.get('processos') or []
    elif isinstance(payload, list):
        processos = payload
    else:
        return []
    if not isinstance(processos, list):
        return []
    return [entry for entry in (_normalize_process_entry(item, 'TRF1') for item in processos) if entry]


def _load_from_pattern(base_dir: Path, pattern: str, tribunal: Optional[str]) -> List[Dict[str, Any]]:
    if not base_dir.exists():
        return []
    results: List[Dict[str, Any]] = []
    for candidate in sorted(base_dir.glob(pattern)):
        with candidate.open('r', encoding='utf-8') as handle:
            payload = json.load(handle)
        processos = payload.get('processos') if isinstance(payload, dict) else payload
        if not isinstance(processos, list):
            continue
        for item in processos:
            if not isinstance(item, dict):
                continue
            entry = _normalize_process_entry(item, tribunal)
            if entry:
                results.append(entry)
    return results


def load_processes(source: Optional[str] = None, tribunal: Optional[str] = None) -> List[Dict[str, Any]]:
    config = load_config()
    path = config.json_path
    resolved_default = Path(__file__).resolve().parents[2] / 'data' / 'integra_service' / 'processos.json'
    explicit_json = path.exists() and str(path.resolve()) != str(resolved_default.resolve())

    if explicit_json and (source is None or source == 'json-explicit'):
        processes = _load_from_json(path)
        if processes:
            return processes

    sources = _iter_process_sources()
    if source:
        sources = [item for item in sources if item['name'] == source]

    for source_cfg in sources:
        if source_cfg['name'] == 'json-explicit':
            continue
        processes = _load_from_pattern(
            source_cfg['path'],
            source_cfg['pattern'],
            tribunal or source_cfg['tribunal'],
        )
        if processes:
            return processes

    return []


def process_one(process_entry: Dict[str, Any], run_id: str | None = None) -> Dict[str, Any]:
    context = get_current_context()
    resolved_run_id = run_id or str(context.get("run_id") or "manual")
    config = load_config()
    return process_process(process_entry, config, resolved_run_id)
