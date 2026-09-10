import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.trf3_mni.models import ReportWindow
from src.trf3_mni.offline import (
    OfflineLocationClient,
    OfflineMniClient,
    OfflineReportClient,
    SYNTHETIC_PROCESS_NUMBERS,
)
from src.trf3_mni.pipeline import BatchRunner
from src.trf3_mni.retry import RetryPolicy
from src.trf3_mni.storage import ManifestStore, PdfStore


class OfflineEndToEndTest(unittest.TestCase):
    def test_offline_batch_creates_and_reuses_two_pdfs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            runner = BatchRunner(
                OfflineReportClient(),
                OfflineMniClient(),
                OfflineLocationClient(),
                PdfStore(output / 'pdfs'),
                ManifestStore(output / 'manifests'),
                RetryPolicy(attempts=1, backoff_seconds=0),
                initial_document_policy='error',
                access_link='offline://pje',
            )
            window = ReportWindow(date(2026, 1, 1), date(2026, 1, 1))

            first = runner.run(window)
            second = runner.run(window)

            self.assertEqual(first.succeeded, 2)
            self.assertEqual(second.succeeded, 2)
            self.assertEqual(
                [result.process_number for result in first.results],
                list(SYNTHETIC_PROCESS_NUMBERS),
            )
            self.assertTrue(all(
                artifact.reused
                for result in second.results
                for artifact in result.artifacts
            ))
            pdfs = list((output / 'pdfs').glob('*.pdf'))
            self.assertEqual(len(pdfs), 2)
            manifest_path = (
                output / 'manifests' / 'manifest-2026-01-01-2026-01-01.json'
            )
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest['summary']['succeeded'], 2)
            self.assertEqual(manifest['summary']['failed'], 0)
