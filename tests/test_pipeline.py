import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.trf3_mni.errors import IntegrationError
from src.trf3_mni.models import (
    DocumentReference,
    DownloadedDocument,
    ProcessMetadata,
    ReportWindow,
    StoredArtifact,
)
from src.trf3_mni.pipeline import BatchRunner, normalize_process_numbers
from src.trf3_mni.retry import RetryPolicy
from src.trf3_mni.storage import ManifestStore


class FakeReportClient:
    def fetch(self, window: ReportWindow):
        return {
            'Resultado': [
                {'nr_processo': '0002-00.00'},
                {'nr_processo': '0001-00.00'},
                {'nr_processo': '0002-00.00'},
            ]
        }


class FakeMniClient:
    def __init__(self) -> None:
        self.query_calls = 0
        self.fetch_calls = 0

    def query_process(self, process_number: str) -> ProcessMetadata:
        self.query_calls += 1
        if self.query_calls == 1:
            raise IntegrationError('falha simulada')
        return ProcessMetadata(
            process_number=process_number,
            filing_date='2026-07-15',
            class_code='100',
            municipality_code='1',
            documents=[DocumentReference(f'doc-{process_number}', '58')],
        )

    def fetch_document(self, process_number, reference):
        self.fetch_calls += 1
        return DownloadedDocument(reference, b'pdf-simulado')


class FakeLocationClient:
    def resolve_state(self, municipality_code: str) -> str:
        return 'sp'


class FakePdfStore:
    def __init__(self, existing: bool = False) -> None:
        self.existing = existing

    def find_existing(self, process, reference, tribunal, state, access_link):
        if not self.existing:
            return None
        return StoredArtifact(
            Path(f'/virtual/{reference.document_id}.pdf'),
            'checksum-existente',
            123,
            reused=True,
        )

    def save(self, process, document, tribunal, state, access_link):
        return StoredArtifact(
            Path(f'/virtual/{document.reference.document_id}.pdf'),
            'checksum',
            len(document.content),
            reused=False,
        )


class PipelineTest(unittest.TestCase):
    def test_normalization_is_unique_and_sorted(self) -> None:
        report = FakeReportClient().fetch(ReportWindow(date.today(), date.today()))
        self.assertEqual(normalize_process_numbers(report), ['00010000', '00020000'])

    def test_batch_retries_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = BatchRunner(
                FakeReportClient(),
                FakeMniClient(),
                FakeLocationClient(),
                FakePdfStore(),
                ManifestStore(Path(temp_dir)),
                RetryPolicy(attempts=2, backoff_seconds=0),
                initial_document_policy='error',
                access_link='https://example.test/pje',
            )
            manifest = runner.run(
                ReportWindow(date(2026, 7, 15), date(2026, 7, 15))
            )

            self.assertEqual(manifest.succeeded, 2)
            self.assertEqual(manifest.failed, 0)
            self.assertTrue(
                (Path(temp_dir) / 'manifest-2026-07-15-2026-07-15.json').is_file()
            )

    def test_existing_artifact_skips_document_download_and_has_duration(self) -> None:
        mni = FakeMniClient()
        runner = BatchRunner(
            report_client=None,
            mni_client=mni,
            location_client=FakeLocationClient(),
            pdf_store=FakePdfStore(existing=True),
            manifest_store=None,
            retry_policy=RetryPolicy(attempts=2, backoff_seconds=0),
            initial_document_policy='error',
            access_link='https://example.test/pje',
        )

        result = runner.process_one('processo-sintetico')

        self.assertEqual(result.status, 'success')
        self.assertEqual(mni.fetch_calls, 0)
        self.assertTrue(result.artifacts[0].reused)
        self.assertIsNotNone(result.started_at)
        self.assertIsNotNone(result.finished_at)
        self.assertIsNotNone(result.duration_seconds)
