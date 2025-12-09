from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from data_platform.etl.auth.certificate_utils import CertificatePair, parse_pem_certificate


@runtime_checkable
class PublicKeyProvider(Protocol):
    """Protocol for classes that can provide public key infrastructure credentials."""

    def get_certificate(self) -> str:
        """Get the certificate content.

        Returns:
        -------
        str
            The certificate content as a string.
        """
        ...

    def get_certificate_pair(self) -> CertificatePair:
        """Get the certificate and private key as a pair.

        Returns:
        -------
        CertificatePair
            The certificate and private key pair.
        """
        ...


@dataclass(slots=True, kw_only=True)
class PkiAuth(PublicKeyProvider):
    """PKI certificate-based authentication.

    Stores certificate and private key content for client certificate authentication.
    """

    cert_secret_key: str

    def get_certificate(self) -> str:
        """Return the certificate content."""
        return self.cert_secret_key

    def get_certificate_pair(self) -> CertificatePair:
        """Return the certificate and private key as a parsed pair."""
        # If we have a combined PEM, parse it
        if self.cert_secret_key:
            return parse_pem_certificate(self.cert_secret_key)

        raise ValueError("No certificate content available")
