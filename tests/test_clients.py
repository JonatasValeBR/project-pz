import unittest
from datetime import date
from types import SimpleNamespace

from src.trf3_mni.clients import LocationClient, MniClient, ReportClient
from src.trf3_mni.models import DocumentReference, ReportWindow


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._data


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.last_call = None

    def post(self, *args, **kwargs):
        self.last_call = ('post', args, kwargs)
        return self.response

    def get(self, *args, **kwargs):
        self.last_call = ('get', args, kwargs)
        return self.response


class FakeMniService:
    def consultarProcesso(self, **kwargs):
        if 'documento' in kwargs:
            document = SimpleNamespace(
                idDocumento=kwargs['documento'], conteudo=b'%PDF-sintetico'
            )
            return SimpleNamespace(
                processo=SimpleNamespace(documento=[document])
            )
        basics = SimpleNamespace(
            dataAjuizamento='20260715',
            classeProcessual='100',
            orgaoJulgador=SimpleNamespace(codigoMunicipioIBGE='3550308'),
        )
        document = SimpleNamespace(idDocumento='doc-1', tipoDocumento='58')
        return SimpleNamespace(
            processo=SimpleNamespace(dadosBasicos=basics, documento=[document])
        )


class ClientsTest(unittest.TestCase):
    def test_report_client_sends_explicit_window_and_timeout(self) -> None:
        session = FakeSession(FakeResponse({'Resultado': []}))
        client = ReportClient('https://example.test', 'token', 421, 7, session)
        client.fetch(ReportWindow(date(2026, 7, 1), date(2026, 7, 2)))

        _, _, kwargs = session.last_call
        self.assertEqual(kwargs['timeout'], 7)
        self.assertEqual(
            kwargs['json']['Data Inicial da Autuação'], '01/07/2026 00:00:00'
        )
        self.assertEqual(
            kwargs['json']['Data Final da Autuação'], '02/07/2026 23:59:59'
        )

    def test_location_client_supports_ibge_shape(self) -> None:
        data = {
            'microrregiao': {
                'mesorregiao': {'UF': {'sigla': 'SP'}}
            }
        }
        session = FakeSession(FakeResponse(data))
        self.assertEqual(LocationClient(5, session).resolve_state('3550308'), 'sp')
        self.assertEqual(session.last_call[2]['timeout'], 5)

    def test_mni_client_maps_metadata_and_document(self) -> None:
        zeep_client = SimpleNamespace(service=FakeMniService())
        client = MniClient(zeep_client, 'login', 'password')

        process = client.query_process('processo-sintetico')
        downloaded = client.fetch_document(
            process.process_number, DocumentReference('doc-1', '58')
        )

        self.assertEqual(process.filing_date, '2026-07-15')
        self.assertEqual(process.municipality_code, '3550308')
        self.assertEqual(downloaded.content, b'%PDF-sintetico')
