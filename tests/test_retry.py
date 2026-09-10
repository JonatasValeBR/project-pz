import unittest

from src.trf3_mni.errors import IntegrationError
from src.trf3_mni.retry import RetryPolicy, call_with_retry


class RetryTest(unittest.TestCase):
    def test_retries_only_until_success(self) -> None:
        calls = []
        sleeps = []

        def operation() -> str:
            calls.append(1)
            if len(calls) < 3:
                raise IntegrationError('transitório')
            return 'ok'

        result = call_with_retry(
            operation,
            RetryPolicy(attempts=3, backoff_seconds=2),
            sleep=sleeps.append,
        )

        self.assertEqual(result, 'ok')
        self.assertEqual(len(calls), 3)
        self.assertEqual(sleeps, [2, 4])

    def test_stops_after_configured_attempts(self) -> None:
        calls = []

        def operation() -> None:
            calls.append(1)
            raise IntegrationError('persistente')

        with self.assertRaises(IntegrationError):
            call_with_retry(
                operation,
                RetryPolicy(attempts=2, backoff_seconds=0),
                sleep=lambda _: None,
            )
        self.assertEqual(len(calls), 2)
