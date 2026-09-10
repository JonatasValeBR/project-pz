"""Adaptadores determinísticos para validar a orquestração sem rede externa."""

import io
from typing import Any, Mapping

import PyPDF2

from .models import (
    DocumentReference,
    DownloadedDocument,
    ProcessMetadata,
    ReportWindow,
)

SYNTHETIC_PROCESS_NUMBERS = (
    '00000010020269000001',
    '00000020020269000002',
)


class OfflineReportClient:
    def fetch(self, window: ReportWindow) -> Mapping[str, Any]:
        del window
        return {
            'Resultado': [
                {'nr_processo': SYNTHETIC_PROCESS_NUMBERS[1]},
                {'nr_processo': SYNTHETIC_PROCESS_NUMBERS[0]},
                {'nr_processo': SYNTHETIC_PROCESS_NUMBERS[1]},
            ]
        }


class OfflineLocationClient:
    def resolve_state(self, municipality_code: str) -> str:
        if municipality_code != '9999999':
            raise ValueError('Município inesperado na fixture offline')
        return 'zz'


class OfflineMniClient:
    def query_process(self, process_number: str) -> ProcessMetadata:
        if process_number not in SYNTHETIC_PROCESS_NUMBERS:
            raise ValueError('Processo inesperado na fixture offline')
        reference = DocumentReference(
            document_id=f'offline-doc-{process_number[-2:]}',
            document_type='58',
        )
        return ProcessMetadata(
            process_number=process_number,
            filing_date='2026-01-01',
            class_code='offline-class',
            municipality_code='9999999',
            documents=[reference],
        )

    def fetch_document(
        self, process_number: str, reference: DocumentReference
    ) -> DownloadedDocument:
        if process_number not in SYNTHETIC_PROCESS_NUMBERS:
            raise ValueError('Processo inesperado na fixture offline')
        return DownloadedDocument(reference=reference, content=_synthetic_pdf())


def _synthetic_pdf() -> bytes:
    writer = PyPDF2.PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.add_metadata({'/fixture': 'trf3-mni-offline'})
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()
