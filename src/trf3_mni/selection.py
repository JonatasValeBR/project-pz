from typing import List

from .errors import DocumentSelectionError
from .models import DocumentReference

INITIAL_PETITION_TYPE = '58'


def select_initial_petitions(
    documents: List[DocumentReference], policy: str = 'error'
) -> List[DocumentReference]:
    matches = [
        document
        for document in documents
        if document.document_type == INITIAL_PETITION_TYPE
    ]
    if not matches:
        raise DocumentSelectionError('Nenhuma petição inicial do tipo 58 encontrada')
    if len(matches) == 1:
        return matches
    if policy == 'first':
        return matches[:1]
    if policy == 'all':
        return matches
    if policy == 'error':
        raise DocumentSelectionError(
            f'Foram encontradas {len(matches)} petições iniciais do tipo 58'
        )
    raise ValueError(f'Política de seleção desconhecida: {policy}')
