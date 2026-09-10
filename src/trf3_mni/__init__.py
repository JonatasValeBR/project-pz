"""Núcleo testável da rotina de relatórios TRF3 e documentos MNI."""

from .config import AppConfig
from .pipeline import BatchRunner

__all__ = ['AppConfig', 'BatchRunner']
