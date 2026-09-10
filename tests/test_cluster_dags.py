import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
CLUSTER_DAGS = {
    "trf3_mni_initial_petitions_cluster_dag.py": "trf3_mni_initial_petitions_cluster",
    "trf1_process_list_cluster_dag.py": "trf1_process_list_cluster",
    "integra_service_downloads_cluster_dag.py": "integra_service_downloads_cluster",
}


class ClusterDagStaticContractTest(unittest.TestCase):
    def test_cluster_variants_have_required_ids_and_discovery_tags(self) -> None:
        for filename, dag_id in CLUSTER_DAGS.items():
            path = ROOT / "dags" / "cluster" / filename
            source = path.read_text(encoding="utf-8")
            ast.parse(source, filename=str(path))
            self.assertIn(f'dag_id="{dag_id}"', source)
            self.assertIn('"unb-pgfn"', source)
            self.assertIn('"unb"', source)

    def test_scraping_variant_uses_cluster_kubernetes_contract(self) -> None:
        source = (
            ROOT / "dags" / "cluster" / "trf1_process_list_cluster_dag.py"
        ).read_text(encoding="utf-8")
        self.assertIn("@task.kubernetes", source)
        self.assertIn('namespace="airflow"', source)
        self.assertIn('key="env"', source)
        self.assertIn('value="svc"', source)
        self.assertIn("airflow-scrapping-base:v6", source)

    def test_local_airflow_ignores_cluster_only_variants(self) -> None:
        ignore = (ROOT / "dags" / ".airflowignore").read_text(encoding="utf-8")
        self.assertIn("cluster/", ignore)

    def test_cluster_dags_do_not_contain_literal_credentials(self) -> None:
        forbidden = ("client_secret=", "password=\"", "totp_secret=\"")
        for filename in CLUSTER_DAGS:
            source = (ROOT / "dags" / "cluster" / filename).read_text(
                encoding="utf-8"
            )
            for token in forbidden:
                self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
