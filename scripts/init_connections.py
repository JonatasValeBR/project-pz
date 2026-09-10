"""Gera arquivo temporário para importação segura de Connections no metastore local."""

import json
import os
from pathlib import Path


def required(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise RuntimeError(f'Variável obrigatória ausente no airflow-init: {name}')
    return value


def main() -> None:
    connections = {
        'trf3_report': {
            'conn_type': 'http',
            'description': 'Relatório gerencial TRF3 (ambiente local)',
            'host': required('TRF3_REPORT_URL'),
            'password': required('TRF3_REPORT_TOKEN'),
            'extra': json.dumps({
                'report_id': int(os.environ.get('TRF3_REPORT_ID', '421')),
                'timeout_seconds': float(
                    os.environ.get('TRF3_HTTP_TIMEOUT_SECONDS', '30')
                ),
            }),
        },
        'trf3_mni': {
            'conn_type': 'http',
            'description': 'MNI TRF3 (ambiente local)',
            'host': required('TRF3_MNI_WSDL'),
            'login': required('TRF3_MNI_LOGIN'),
            'password': required('TRF3_MNI_PASSWORD'),
            'extra': json.dumps({
                'access_link': required('TRF3_ACCESS_LINK'),
                'wsdl_timeout_seconds': float(
                    os.environ.get('TRF3_SOAP_WSDL_TIMEOUT_SECONDS', '30')
                ),
                'operation_timeout_seconds': float(
                    os.environ.get('TRF3_SOAP_OPERATION_TIMEOUT_SECONDS', '60')
                ),
                'retry_attempts': int(
                    os.environ.get('TRF3_RETRY_ATTEMPTS', '3')
                ),
                'retry_backoff_seconds': float(
                    os.environ.get('TRF3_RETRY_BACKOFF_SECONDS', '2')
                ),
                'initial_document_policy': os.environ.get(
                    'TRF3_INITIAL_DOCUMENT_POLICY', 'error'
                ),
            }),
        },
        'trf1_pje': {
            'conn_type': 'http',
            'description': 'PJe TRF1 para scraping da lista de processos',
            'host': required('TRF1_ENDPOINT'),
            'login': required('TRF1_LOGIN'),
            'password': required('TRF1_PASSWORD'),
            'extra': json.dumps({
                'totp_secret': required('TRF1_TOTP_SECRET'),
                'reports_url': required('TRF1_REPORTS_URL'),
                'timeout_seconds': int(
                    os.environ.get('TRF1_TIMEOUT_SECONDS', '60')
                ),
                'headless': os.environ.get(
                    'TRF1_HEADLESS', 'true'
                ).strip().lower() not in {'0', 'false', 'no'},
            }),
        },
    }
    output = Path('/tmp/pgfn-connections.json')
    output.write_text(json.dumps(connections), encoding='utf-8')
    output.chmod(0o600)


if __name__ == '__main__':
    main()
