import unittest

from src.trf3_mni.errors import DocumentSelectionError
from src.trf3_mni.models import DocumentReference
from src.trf3_mni.selection import select_initial_petitions


class SelectionTest(unittest.TestCase):
    def test_selects_only_type_58(self) -> None:
        documents = [
            DocumentReference('outro', '1'),
            DocumentReference('inicial', '58'),
        ]
        self.assertEqual(
            select_initial_petitions(documents),
            [DocumentReference('inicial', '58')],
        )

    def test_default_policy_rejects_ambiguity(self) -> None:
        documents = [
            DocumentReference('a', '58'),
            DocumentReference('b', '58'),
        ]
        with self.assertRaises(DocumentSelectionError):
            select_initial_petitions(documents)

    def test_all_policy_keeps_every_initial_petition(self) -> None:
        documents = [
            DocumentReference('a', '58'),
            DocumentReference('b', '58'),
        ]
        self.assertEqual(select_initial_petitions(documents, 'all'), documents)
