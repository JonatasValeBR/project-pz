import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


try:
    # O pacote Apache Airflow pode não estar instalado diretamente no host.
    AIRFLOW_AVAILABLE = importlib.util.find_spec('airflow.sdk') is not None
except ModuleNotFoundError:
    AIRFLOW_AVAILABLE = False


@unittest.skipUnless(AIRFLOW_AVAILABLE, 'Airflow é validado dentro da imagem Docker')
class AirflowDagContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from airflow.dag_processing.dagbag import DagBag

        container_dags = Path('/opt/airflow/dags')
        local_dags = Path(__file__).resolve().parents[1] / 'dags'
        cls.dag_bag = DagBag(str(container_dags if container_dags.exists() else local_dags))
        cls.dag = cls.dag_bag.get_dag('trf3_mni_initial_petitions')
        cls.recovery_dag = cls.dag_bag.get_dag('trf3_mni_retry_failures')
        cls.trf1_dag = cls.dag_bag.get_dag('trf1_process_list')
        dag_file = container_dags / 'trf3_mni_dag.py'
        if not dag_file.exists():
            dag_file = local_dags / 'trf3_mni_dag.py'
        spec = importlib.util.spec_from_file_location('trf3_mni_dag_test', dag_file)
        cls.dag_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.dag_module)
        recovery_file = container_dags / 'trf3_mni_recovery_dag.py'
        if not recovery_file.exists():
            recovery_file = local_dags / 'trf3_mni_recovery_dag.py'
        recovery_spec = importlib.util.spec_from_file_location(
            'trf3_mni_recovery_dag_test', recovery_file
        )
        cls.recovery_module = importlib.util.module_from_spec(recovery_spec)
        recovery_spec.loader.exec_module(cls.recovery_module)

    def test_imports_without_errors(self) -> None:
        self.assertEqual(self.dag_bag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertIsNotNone(self.recovery_dag)
        self.assertIsNotNone(self.trf1_dag)

    def test_trf1_is_manual_serial_and_has_no_pdf_task(self) -> None:
        self.assertIsNone(self.trf1_dag.schedule)
        self.assertFalse(self.trf1_dag.catchup)
        self.assertEqual(self.trf1_dag.max_active_runs, 1)
        self.assertEqual(set(self.trf1_dag.task_ids), {'scrape_processes'})
        self.assertEqual(
            self.trf1_dag.get_task('scrape_processes').pool,
            'trf1_pje_pool',
        )

    def test_is_manual_and_bounded(self) -> None:
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertIn('max_processes', self.dag.params)
        self.assertIn('retry_attempts', self.dag.params)
        self.assertIn('retry_backoff_seconds', self.dag.params)

    def test_task_graph_and_mapping_contract(self) -> None:
        self.assertEqual(
            set(self.dag.task_ids),
            {
                'resolve_window',
                'fetch_processes',
                'select_processes',
                'process_one',
                'publish_manifest',
                'quality_gate',
            },
        )
        mapped = self.dag.get_task('process_one')
        self.assertEqual(type(mapped).__name__, 'DecoratedMappedOperator')
        self.assertEqual(mapped.pool, 'mni_pool')

    def test_offline_config_does_not_read_connections(self) -> None:
        from airflow.sdk import Connection

        with patch.object(
            Connection,
            'get',
            side_effect=AssertionError('Connection não deve ser lida offline'),
        ):
            config = self.dag_module._load_config(offline=True)
        self.assertEqual(config.report_url, 'offline://report')
        self.assertEqual(config.retry_attempts, 1)

    def test_process_limit_is_deterministic_and_validated(self) -> None:
        processes = [str(index) for index in range(20)]
        self.assertEqual(
            self.dag_module._limit_processes(processes, '15'),
            processes[:15],
        )
        with self.assertRaises(ValueError):
            self.dag_module._limit_processes(processes, 0)

    def test_retry_settings_allow_per_run_override_and_validate(self) -> None:
        self.assertEqual(
            self.dag_module._retry_settings(3, 2.0, '2', '1.5'),
            (2, 1.5),
        )
        self.assertEqual(
            self.dag_module._retry_settings(3, 2.0, None, None),
            (3, 2.0),
        )
        with self.assertRaises(ValueError):
            self.dag_module._retry_settings(3, 2.0, 0, None)

    def test_recovery_dag_is_manual_bounded_and_mapped(self) -> None:
        self.assertIsNone(self.recovery_dag.schedule)
        self.assertFalse(self.recovery_dag.catchup)
        self.assertEqual(self.recovery_dag.max_active_runs, 1)
        self.assertIn('source_manifest', self.recovery_dag.params)
        self.assertIn('error_types', self.recovery_dag.params)
        self.assertEqual(
            set(self.recovery_dag.task_ids),
            {
                'load_failed_processes',
                'select_processes',
                'process_one',
                'publish_recovery_manifest',
                'quality_gate',
            },
        )
        self.assertEqual(self.recovery_dag.get_task('process_one').pool, 'mni_pool')

    def test_recovery_selects_only_failed_processes_deterministically(self) -> None:
        manifest = {
            'results': [
                {'process_number': '0002-00', 'status': 'failed'},
                {'process_number': '0001-00', 'status': 'success'},
                {'process_number': '0003-00', 'status': 'failed'},
                {'process_number': '0002.00', 'status': 'failed'},
            ]
        }
        self.assertEqual(
            self.recovery_module._failed_processes(manifest),
            ['000200', '000300'],
        )
        self.assertEqual(
            self.recovery_module._failed_processes(manifest, 1),
            ['000200'],
        )

    def test_recovery_rejects_unsafe_manifest_names(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with self.assertRaises(ValueError):
                self.recovery_module._resolve_manifest_path(root, '../manifest.json')
            with self.assertRaises(ValueError):
                self.recovery_module._resolve_manifest_path(root, '/tmp/manifest.json')

    def test_recovery_filters_error_types(self) -> None:
        manifest = {
            'results': [
                {
                    'process_number': '0001-00',
                    'status': 'failed',
                    'error_type': 'StorageError',
                },
                {
                    'process_number': '0002-00',
                    'status': 'failed',
                    'error_type': 'ProcessNotFoundError',
                },
            ]
        }
        self.assertEqual(
            self.recovery_module._failed_processes(
                manifest,
                raw_error_types=['StorageError'],
            ),
            ['000100'],
        )
        with self.assertRaises(ValueError):
            self.recovery_module._failed_processes(
                manifest,
                raw_error_types=['DocumentSelectionError'],
            )
        with self.assertRaises(ValueError):
            self.recovery_module._normalize_error_types(['../StorageError'])
