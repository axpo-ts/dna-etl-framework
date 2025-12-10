from __future__ import annotations

import datetime
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, ClassVar, cast

import httpx

from etl.auth.token_provider import TokenProvider
from etl.core.retry import RetryConfig, wrapper_retry


@dataclass(slots=True, kw_only=True)
class BaseOauth(ABC, TokenProvider):
    """Abstract base for OAuth-style credentials."""

    auth_url: str  #: Where to obtain the *access_token* (full URL).
    content_type: str = "application/x-www-form-urlencoded"
    retry_cfg: RetryConfig = field(default_factory=RetryConfig)  # NEW
    _TOKEN_STORE: ClassVar[dict[str, tuple[str, datetime.datetime]]] = {}

    def _cache_key(self) -> str:
        # client_id is present on Client-Credentials flows;
        # fall back to auth_url only for username/password
        client_id = getattr(self, "client_id", "") or getattr(self, "username", "")
        return f"{self.auth_url}:{client_id}"

    def _post_with_retry(self, *, data: Any, headers: dict[str, str], timeout: int, verify: bool) -> httpx.Response:
        @wrapper_retry(self.retry_cfg)
        def _call() -> httpx.Response:
            if self.content_type == "application/json":
                return httpx.post(
                    self.auth_url,
                    json=data,
                    headers=headers,
                    timeout=timeout,
                    verify=verify,
                )
            else:
                return httpx.post(
                    self.auth_url,
                    data=data,
                    headers=headers,
                    timeout=timeout,
                    verify=verify,
                )

        return _call()

    def fetch_token(self, *, timeout: int, verify: bool = True) -> str:
        """Fetch an access token from the authentication server.

        Args:
            timeout : int
                The timeout in seconds for the HTTP request.
            verify : bool
                Whether to verify the server's TLS certificate (default: True).

        Returns:
            str The access token as a string.

        Raises:
            ValueError
                If the access token is missing in the response.
            httpx.HTTPStatusError
                If the HTTP request fails.
        """
        key = self._cache_key()
        cached = self._TOKEN_STORE.get(key)

        # ── 1. Return the cached token if still valid (5-min safety margin)
        if cached:
            token, expires_at = cached
            if expires_at - timedelta(minutes=5) > datetime.datetime.now(datetime.UTC):
                return token

        # ── 2. Otherwise hit the auth endpoint
        data, headers, token_key = self._build_request()
        resp = self._post_with_retry(data=data, headers=headers, timeout=timeout, verify=verify)
        resp.raise_for_status()

        body = resp.json()
        # Some APIs (i.e. Nena) uses the all returned JSON as the token.
        if not token_key:
            token = body
        else:
            token = body[token_key]
        expires_in = 3600
        if isinstance(body, dict):
            expires_in = int(body.get("expires_in", 3600))  # seconds

        # ── 3. Cache the new token
        expires_at = datetime.datetime.now(datetime.UTC) + timedelta(seconds=expires_in)
        self._TOKEN_STORE[key] = (token, expires_at)
        return cast(str, token)

    @abstractmethod
    def _build_request(self) -> tuple[Any, str, str]:
        """Return (*data*, *content_type*, *token_key*)."""


@dataclass(slots=True, kw_only=True)
class BasicOauth(BaseOauth):
    """Username + password exchanged for a bearer token.

    Example usage
    -------------
    >>> creds = BasicOauth(
    ...     auth_url="https://api.example.com/auth/login",
    ...     username="alice",
    ...     password="s3cret",
    ... )
    """

    username: str
    password: str
    #: JSON key that holds the token in the auth response (default *access_token*)
    auth_token_name: str = "access_token"

    def _build_request(self) -> tuple[Any, str, str]:
        if not (self.username and self.password):
            raise ValueError("username and password are required")
        payload = json.dumps({"login": self.username, "password": self.password})
        return payload, self.content_type, self.auth_token_name


@dataclass(slots=True, kw_only=True)
class AzureOauth(BaseOauth):
    """Azure Active Directory client-credentials (service-to-service) flow.

    All four fields are required; the reader constructs the final token-endpoint
    URL as `https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token`.
    """

    tenant_id: str = "8619c67c-945a-48ae-8e77-35b1b71c9b98"
    client_id: str
    client_secret: str
    api_scope: str  #: e.g. "https://graph.microsoft.com/.default"

    # The auth_url is derived, but we keep the field from Base for uniformity.
    def __post_init__(self) -> None:
        """Set the auth_url for AzureOauth after initialization."""
        object.__setattr__(
            self,
            "auth_url",
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
        )

    def _build_request(self) -> tuple[Any, str, str]:
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.api_scope,
        }
        headers = {"Content-Type": self.content_type}
        return payload, headers, "access_token"


@dataclass(slots=True, kw_only=True)
class PasswordGrantOauth(BaseOauth):
    """Standard *grant_type=password* flow.

    Example payload::
        grant_type=password&client_id=...&username=...&password=...
    """

    username: str
    password: str

    # optional extras
    client_id: str | None = None  # e.g. "client_marketdata_api"
    client_secret: str | None = None  # ff the app is a confidential client, then it must be included
    scope: str | None = None
    basic_auth_b64: str | None = None  # pre-encoded "Basic …" header if the server requires it

    # names & formats
    auth_token_name: str = "access_token"
    grant_type: str = "password"

    # ---------- helpers ----------
    def _build_request(self) -> tuple[Any, dict[str, str], str]:
        """Return (<payload>, <headers>, <json-key-holding-the-token>)."""
        payload: dict[str, str] = {
            "grant_type": self.grant_type,
            "username": self.username,
            "password": self.password,
        }
        if self.scope:
            payload["scope"] = self.scope
        if self.client_id:
            payload["client_id"] = self.client_id
        if self.client_secret:
            payload["client_secret"] = self.client_secret

        headers = {"Content-Type": self.content_type}
        if self.basic_auth_b64:
            headers["Authorization"] = f"Basic {self.basic_auth_b64}"

        return payload, headers, self.auth_token_name


__all__ = [
    "AzureOauth",
    "BaseOauth",
    "BasicOauth",
    "PasswordGrantOauth",
]
