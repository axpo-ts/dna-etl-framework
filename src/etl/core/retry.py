from dataclasses import dataclass

from tenacity import retry, stop_after_attempt, wait_random_exponential


@dataclass(slots=True)
class RetryConfig:
    """Grouping of tenacity-retry knobs for HTTP calls."""

    max_attempts: int = 5  # stop_after_attempt
    wait_min_sec: int = 2  # wait_random_exponential → min
    wait_max_sec: int = 30  # wait_random_exponential → max
    wait_multiplier: float = 2.0  # wait_random_exponential → multiplier


def wrapper_retry(retry_cfg: RetryConfig) -> any:
    """Decorator to apply retry configuration to a function."""
    return retry(
        stop=stop_after_attempt(retry_cfg.max_attempts),
        wait=wait_random_exponential(
            multiplier=retry_cfg.wait_multiplier,
            min=retry_cfg.wait_min_sec,
            max=retry_cfg.wait_max_sec,
        ),
        reraise=True,
    )
