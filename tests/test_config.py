import os
import unittest
from pathlib import Path
from unittest.mock import patch

from src.trf3_mni.config import AppConfig
from src.trf3_mni.errors import ConfigurationError


BASE_ENV = {
    'TOKEN': 'token-local-de-teste',
    'ENDPOINT_TRF3': 'https://example.test/mni?wsdl',
    'LINK_ACESSO_TRF3': 'https://example.test/pje',
    'LOGIN': 'login-local-de-teste',
    'SENHA': 'senha-local-de-teste',
    'PATH_PDF': '/tmp/pdfs-de-teste',
}


class AppConfigTest(unittest.TestCase):
    def test_loads_required_values_without_dotenv(self) -> None:
        with patch.dict(os.environ, BASE_ENV, clear=True):
            config = AppConfig.from_env(None)

        self.assertEqual(config.report_id, 421)
        self.assertEqual(config.pdf_dir, Path('/tmp/pdfs-de-teste'))
        self.assertEqual(config.initial_document_policy, 'error')
        self.assertNotIn('senha-local-de-teste', repr(config))
        self.assertNotIn('token-local-de-teste', repr(config))

    def test_rejects_missing_secret(self) -> None:
        values = dict(BASE_ENV)
        values.pop('SENHA')
        with patch.dict(os.environ, values, clear=True):
            with self.assertRaisesRegex(ConfigurationError, 'SENHA'):
                AppConfig.from_env(None)

    def test_rejects_unknown_document_policy(self) -> None:
        values = {**BASE_ENV, 'INITIAL_DOCUMENT_POLICY': 'silencioso'}
        with patch.dict(os.environ, values, clear=True):
            with self.assertRaises(ConfigurationError):
                AppConfig.from_env(None)
