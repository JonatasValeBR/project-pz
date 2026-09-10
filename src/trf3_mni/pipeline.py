import logging
import re
from datetime import datetime, timezone
from typing import Any, List, Mapping

from .errors import ApplicationError, InvalidResponseError
from .models import BatchManifest, ProcessResult, ReportWindow
from .retry import RetryPolicy, call_with_retry
from .selection import select_initial_petitions

LOGGER = logging.getLogger(__name__)


def normalize_process_numbers(report: Mapping[str, Any]) -> List[str]:
    result = report.get('Resultado')
    if not isinstance(result, list):
        raise InvalidResponseError('Relatório não possui uma lista Resultado')

    process_numbers = set()
    for item in result:
        if not isinstance(item, dict):
            raise InvalidResponseError('Relatório contém um item inválido')
        raw = str(item.get('nr_processo', ''))
        normalized = re.sub(r'\D', '', raw)
        if not normalized:
            raise InvalidResponseError('Relatório contém processo sem número válido')
        process_numbers.add(normalized)
    return sorted(process_numbers)


class BatchRunner:
    def __init__(
        self,
        report_client: Any,
        mni_client: Any,
        location_client: Any,
        pdf_store: Any,
        manifest_store: Any,
        retry_policy: RetryPolicy,
        initial_document_policy: str,
        access_link: str,
        tribunal: str = 'TRF 3',
    ) -> None:
        self._report_client = report_client
        self._mni_client = mni_client
        self._location_client = location_client
        self._pdf_store = pdf_store
        self._manifest_store = manifest_store
        self._retry_policy = retry_policy
        self._initial_document_policy = initial_document_policy
        self._access_link = access_link
        self._tribunal = tribunal

    def run(self, window: ReportWindow) -> BatchManifest:
        started_at = datetime.now(timezone.utc)
        report = self._retry(lambda: self._report_client.fetch(window), 'relatório')
        process_numbers = normalize_process_numbers(report)
        LOGGER.info('Relatório normalizado com %d processos', len(process_numbers))

        results = [self.process_one(number) for number in process_numbers]
        manifest = BatchManifest(
            window=window,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            results=results,
        )
        path = self._manifest_store.save(manifest)
        LOGGER.info(
            'Manifesto salvo em %s: %d sucessos, %d falhas',
            path,
            manifest.succeeded,
            manifest.failed,
        )
        return manifest

    def process_one(self, process_number: str) -> ProcessResult:
        """Processa um item isolado; ponto de entrada para futuro task mapping."""
        started_at = datetime.now(timezone.utc)

        def result(status: str, **kwargs: Any) -> ProcessResult:
            finished_at = datetime.now(timezone.utc)
            return ProcessResult(
                process_number=process_number,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=(finished_at - started_at).total_seconds(),
                **kwargs,
            )

        try:
            process = self._retry(
                lambda: self._mni_client.query_process(process_number),
                'consulta MNI',
            )
            references = select_initial_petitions(
                process.documents, self._initial_document_policy
            )
            state = self._retry(
                lambda: self._location_client.resolve_state(
                    process.municipality_code
                ),
                'consulta IBGE',
            )
            artifacts = []
            for reference in references:
                existing = self._pdf_store.find_existing(
                    process,
                    reference,
                    tribunal=self._tribunal,
                    state=state,
                    access_link=self._access_link,
                )
                if existing is not None:
                    artifacts.append(existing)
                    LOGGER.info('PDF local íntegro reutilizado; download ignorado')
                    continue
                document = self._retry(
                    lambda ref=reference: self._mni_client.fetch_document(
                        process_number, ref
                    ),
                    'download MNI',
                )
                artifacts.append(
                    self._pdf_store.save(
                        process,
                        document,
                        tribunal=self._tribunal,
                        state=state,
                        access_link=self._access_link,
                    )
                )
            LOGGER.info('Processo concluído com %d documento(s)', len(artifacts))
            return result('success', artifacts=artifacts)
        except ApplicationError as exc:
            LOGGER.warning('Processo falhou: %s', exc.__class__.__name__)
            return result(
                'failed',
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )
        except Exception:
            LOGGER.exception('Processo falhou com erro não classificado')
            return result(
                'failed',
                error_type='UnexpectedError',
                error_message='Erro inesperado não classificado',
            )

    def _retry(self, operation: Any, operation_name: str) -> Any:
        return call_with_retry(
            operation,
            self._retry_policy,
            on_retry=lambda exc, attempt: LOGGER.warning(
                'Falha transitória em %s; tentativa %d/%d',
                operation_name,
                attempt,
                self._retry_policy.attempts,
            ),
        )
