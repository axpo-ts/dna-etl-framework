from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@runtime_checkable
class TokenProvider(Protocol):
    """Protocol for classes that can fetch an access token."""

    def fetch_token(self, *, timeout: float) -> str:
        """Fetch an access token from the authentication server.

        Parameters
        ----------
        timeout : float
            The timeout in seconds for the HTTP request.

        Returns:
        -------
        str
            The access token as a string.
        """
        ...


@dataclass(slots=True, kw_only=True)
class ApiKeyAuth(TokenProvider):
    """Static API-key credential.

    The key is injected in every request header as

        {_HEADER_NAME: <api_key>}

    No network round-trip is needed, so *timeout* is ignored.
    """

    api_key: str

    def fetch_token(self, *, timeout: float = 0) -> str:
        """Return the API key as the access token."""
        return self.api_key
