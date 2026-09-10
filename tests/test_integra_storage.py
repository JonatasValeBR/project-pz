import json
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from src.integra_service.workflow import process_process
from src.pgfn_storage import ObjectStorage


class FakeIntegraClient:
    def __init__(self, config, logger=None) -> None:
        pass

    def get_token(self) -> str:
        return "token-sintetico"

    def submit_dados_basicos(self, token, tribunal, process_number) -> int:
        return 1

    def poll_status(self, token, job_id, url_template) -> dict:
        return {"estado": "RETORNADA_TRF"}

    def get_dados_retorno(self, token, job_id) -> dict:
        return {"bucketSolicitacao": "ceph", "identificadorCeph": "dados"}

    def download_ceph(self, token, bucket, object_name) -> bytes:
        if object_name == "dados":
            return json.dumps({
                "dadosBasicos": {"classe": "teste"},
                "documentos": [{"idDocumento": "58", "descricao": "PETIÇÃO INICIAL"}],
            }).encode("utf-8")
        return b"%PDF-1.3\n%%EOF"

    def extract_document_id(self, payload) -> str:
        return "58"

    def submit_pecas(self, token, tribunal, process_number, document_id) -> int:
        return 2

    def get_pecas_retorno(self, token, job_id) -> dict:
        return {"bucket": "ceph", "identificadorCeph": "pdf"}


class IntegraObjectStorageTest(unittest.TestCase):
    def test_workflow_writes_all_artifacts_to_memory_bucket(self) -> None:
        root = f"memory://integra-tests/{uuid.uuid4().hex}"
        config = SimpleNamespace(
            downloads_dir=root,
            dados_status_url="dados/{job_id}",
            pecas_status_url="pecas/{job_id}",
        )
        with patch("src.integra_service.workflow.IntegraClient", FakeIntegraClient):
            result = process_process(
                {"numero_processo": "0001", "link_tribunal": "TRF1"},
                config,
                "run-1",
            )

        storage = ObjectStorage(root)
        self.assertEqual(result["status"], "success")
        self.assertTrue(storage.exists("TRF1/0001/dadosBasicos.json"))
        self.assertTrue(storage.exists("TRF1/0001/dadosBasicos_full.json"))
        self.assertTrue(storage.exists("TRF1/0001/peticao_inicial.pdf"))
        self.assertTrue(storage.exists("TRF1/0001/manifest.json"))


if __name__ == "__main__":
    unittest.main()
