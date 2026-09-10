import unittest

from src.trf1_scraping.scraper import Trf1ProcessListScraper


class Trf1ScraperTest(unittest.TestCase):
    def test_normalizes_portuguese_filed_date(self) -> None:
        self.assertEqual(
            Trf1ProcessListScraper._normalize_filed_date("7 jul 2026"),
            "07/07/2026",
        )

    def test_rejects_unexpected_filed_date(self) -> None:
        with self.assertRaises(ValueError):
            Trf1ProcessListScraper._normalize_filed_date("2026-07-07")

