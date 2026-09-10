import time
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, Type, TypeVar

from .errors import IntegrationError

T = TypeVar('T')


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    backoff_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.attempts <= 0:
            raise ValueError('attempts deve ser maior que zero')
        if self.backoff_seconds < 0:
            raise ValueError('backoff_seconds não pode ser negativo')


def call_with_retry(
    operation: Callable[[], T],
    policy: RetryPolicy,
    retry_on: Tuple[Type[BaseException], ...] = (IntegrationError,),
    on_retry: Optional[Callable[[BaseException, int], None]] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    for attempt in range(1, policy.attempts + 1):
        try:
            return operation()
        except retry_on as exc:
            if attempt == policy.attempts:
                raise
            if on_retry:
                on_retry(exc, attempt)
            sleep(policy.backoff_seconds * (2 ** (attempt - 1)))
    raise AssertionError('Fluxo de retry terminou sem resultado')
