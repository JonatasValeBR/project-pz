import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class IntegraServiceConfigTest(unittest.TestCase):
    def test_loads_defaults_and_env_overrides(self) -> None:
        from src.integra_service.config import load_config

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            with patch.dict(os.environ, {
                'INTEGRA_CLIENT_ID': 'client-id',
                'INTEGRA_CLIENT_SECRET': 'client-secret',
                'INTEGRA_DOWNLOADS_DIR': str(tmp_path / 'downloads'),
                'INTEGRA_RUNS_DIR': str(tmp_path / 'runs'),
                'INTEGRA_JSON_PATH': str(tmp_path / 'processos.json'),
                'INTEGRA_SERVICE_BASE_URL': 'https://integra.test',
                'INTEGRA_KC_BASE_URL': 'https://keycloak.test',
                'INTEGRA_KC_REALM': 'integra-realm',
            }, clear=False):
                config = load_config()

        self.assertEqual(config.client_id, 'client-id')
        self.assertEqual(config.client_secret, 'client-secret')
        self.assertEqual(str(config.downloads_dir), str(tmp_path / 'downloads'))
        self.assertEqual(config.service_base_url, 'https://integra.test')
        self.assertEqual(config.token_url, 'https://keycloak.test/realms/integra-realm/protocol/openid-connect/token')


class IntegraServiceDagContractTest(unittest.TestCase):
    def test_dag_contains_expected_tasks(self) -> None:
        from airflow.sdk import dag  # noqa: F401

        dag_path = Path(__file__).resolve().parents[1] / 'dags' / 'integra_service_dag.py'
        if not dag_path.exists():
            self.fail('DAG de Integra não existe')

        spec = importlib.util.spec_from_file_location('integra_service_dag_test', dag_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        dag_obj = module.integra_service_downloads
        self.assertEqual(
            set(dag_obj.task_ids),
            {'resolve_window', 'load_processes', 'process_processes', 'publish_manifest', 'quality_gate'},
        )
