import base64
import binascii
from typing import Any, Dict, Mapping, Optional

import requests
import zeep
from requests import Session
from zeep.transports import Transport

from .errors import IntegrationError, InvalidResponseError, ProcessNotFoundError
from .models import (
    DocumentReference,
    DownloadedDocument,
    ProcessMetadata,
    ReportWindow,
)


class ReportClient:
    def __init__(
        self,
        url: str,
        token: str,
        report_id: int,
        timeout_seconds: float,
        session: Optional[Session] = None,
    ) -> None:
        self._url = url
        self._token = token
        self._report_id = report_id
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()

    def fetch(self, window: ReportWindow) -> Mapping[str, Any]:
        payload = {
            'Token': self._token,
            'Id': self._report_id,
            'Data Inicial da Autuação': window.start.strftime('%d/%m/%Y 00:00:00'),
            'Data Final da Autuação': window.end.strftime('%d/%m/%Y 23:59:59'),
        }
        try:
            response = self._session.post(
                self._url,
                headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
                json=payload,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise IntegrationError('Falha ao consultar o relatório gerencial do TRF3') from exc

        if not isinstance(data, dict) or not isinstance(data.get('Resultado'), list):
            raise InvalidResponseError('Relatório TRF3 retornou uma estrutura inválida')
        return data


class LocationClient:
    def __init__(
        self, timeout_seconds: float, session: Optional[Session] = None
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()

    def resolve_state(self, municipality_code: str) -> str:
        url = (
            'https://servicodados.ibge.gov.br/api/v1/localidades/municipios/'
            f'{municipality_code}'
        )
        try:
            response = self._session.get(url, timeout=self._timeout_seconds)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise IntegrationError('Falha ao consultar a localização no IBGE') from exc

        paths = (
            ('microrregiao', 'mesorregiao', 'UF', 'sigla'),
            ('regiao-imediata', 'regiao-intermediaria', 'UF', 'sigla'),
        )
        for path in paths:
            value: Any = data
            for key in path:
                if not isinstance(value, dict):
                    value = None
                    break
                value = value.get(key)
            if isinstance(value, str) and len(value) == 2:
                return value.lower()
        raise InvalidResponseError('IBGE não retornou uma UF reconhecível')


class MniClient:
    def __init__(self, client: zeep.Client, login: str, password: str) -> None:
        self._client = client
        self._login = login
        self._password = password

    @classmethod
    def connect(
        cls,
        wsdl: str,
        login: str,
        password: str,
        wsdl_timeout_seconds: float,
        operation_timeout_seconds: float,
    ) -> 'MniClient':
        try:
            transport = Transport(
                timeout=wsdl_timeout_seconds,
                operation_timeout=operation_timeout_seconds,
            )
            settings = zeep.Settings(
                strict=False,
                xml_huge_tree=True,
                xsd_ignore_sequence_order=True,
            )
            client = zeep.Client(wsdl, settings=settings, transport=transport)
        except Exception as exc:
            raise IntegrationError('Falha ao criar o cliente MNI') from exc
        return cls(client, login, password)

    def query_process(self, process_number: str) -> ProcessMetadata:
        try:
            response = self._client.service.consultarProcesso(
                idConsultante=self._login,
                senhaConsultante=self._password,
                numeroProcesso=process_number,
                incluirDocumentos=True,
            )
        except Exception as exc:
            raise IntegrationError('Falha ao consultar processo no MNI') from exc

        process = getattr(response, 'processo', None)
        if process is None:
            raise ProcessNotFoundError('Processo não encontrado no MNI')
        basics = getattr(process, 'dadosBasicos', None)
        court = getattr(basics, 'orgaoJulgador', None)
        if basics is None or court is None:
            raise InvalidResponseError('MNI retornou dados básicos incompletos')

        filing_date = self._format_date(getattr(basics, 'dataAjuizamento', ''))
        class_code = str(getattr(basics, 'classeProcessual', '') or '')
        municipality_code = str(getattr(court, 'codigoMunicipioIBGE', '') or '')
        if not class_code or not municipality_code:
            raise InvalidResponseError('MNI retornou classe ou município ausente')

        documents = []
        for document in (getattr(process, 'documento', None) or []):
            document_id = str(getattr(document, 'idDocumento', '') or '')
            document_type = str(getattr(document, 'tipoDocumento', '') or '')
            if document_id:
                documents.append(DocumentReference(document_id, document_type))

        return ProcessMetadata(
            process_number=process_number,
            filing_date=filing_date,
            class_code=class_code,
            municipality_code=municipality_code,
            documents=documents,
        )

    def fetch_document(
        self, process_number: str, reference: DocumentReference
    ) -> DownloadedDocument:
        try:
            response = self._client.service.consultarProcesso(
                idConsultante=self._login,
                senhaConsultante=self._password,
                numeroProcesso=process_number,
                documento=reference.document_id,
            )
        except Exception as exc:
            raise IntegrationError('Falha ao baixar documento no MNI') from exc

        process = getattr(response, 'processo', None)
        documents = getattr(process, 'documento', None) if process is not None else None
        if not documents:
            raise InvalidResponseError('MNI não retornou o documento solicitado')

        selected = next(
            (
                item
                for item in documents
                if str(getattr(item, 'idDocumento', '') or '') == reference.document_id
            ),
            documents[0] if len(documents) == 1 else None,
        )
        content = getattr(selected, 'conteudo', None) if selected is not None else None
        if isinstance(content, str):
            try:
                content = base64.b64decode(content, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise InvalidResponseError('MNI retornou conteúdo inválido') from exc
        if not isinstance(content, bytes) or not content:
            raise InvalidResponseError('MNI retornou documento sem conteúdo')
        return DownloadedDocument(reference=reference, content=content)

    @staticmethod
    def _format_date(value: Any) -> str:
        raw = str(value or '')
        if len(raw) < 8 or not raw[:8].isdigit():
            raise InvalidResponseError('MNI retornou data de ajuizamento inválida')
        return f'{raw[:4]}-{raw[4:6]}-{raw[6:8]}'
