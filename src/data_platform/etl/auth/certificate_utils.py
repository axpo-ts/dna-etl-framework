"""Certificate utilities for parsing and managing PEM certificates and keys."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass

import pem  # type: ignore[import]


@dataclass(slots=True)
class CertificatePair:
    """Container for certificate and private key content."""

    certificate: str
    private_key: str


def parse_pem_certificate(cert_content: str) -> CertificatePair:
    """Parse a PEM certificate string containing both certificate and private key.

    Args:
        cert_content: The PEM certificate content as a string

    Returns:
        CertificatePair: Object containing separated certificate and private key

    Raises:
        ValueError: If the certificate content cannot be parsed or is missing required components
    """
    try:
        # Parse the PEM content
        pem_objects = pem.parse(cert_content.encode("utf-8"))

        certificate = ""
        private_key = ""

        for obj in pem_objects:
            if isinstance(obj, pem.Certificate):
                certificate = obj.as_text()
            elif isinstance(obj, pem.PrivateKey):
                private_key = obj.as_text()

        if not certificate:
            raise ValueError("No certificate found in PEM content")
        if not private_key:
            raise ValueError("No private key found in PEM content")

        return CertificatePair(certificate=certificate, private_key=private_key)

    except Exception as e:
        raise ValueError(f"Failed to parse PEM certificate: {e}") from e


def create_temp_certificate_files(cert_pair: CertificatePair) -> tuple[str, str]:
    """Create temporary files for certificate and private key.

    Args:
        cert_pair: The certificate pair containing certificate and private key

    Returns:
        tuple[str, str]: Paths to the temporary certificate and key files
    """
    cert_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
    key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)

    try:
        cert_file.write(cert_pair.certificate)
        cert_file.flush()

        key_file.write(cert_pair.private_key)
        key_file.flush()

        return cert_file.name, key_file.name
    finally:
        cert_file.close()
        key_file.close()


def cleanup_temp_files(*file_paths: str) -> None:
    """Clean up temporary files.

    Args:
        *file_paths: Paths to files to be deleted
    """
    import os

    for file_path in file_paths:
        try:
            if file_path and os.path.exists(file_path):
                os.unlink(file_path)
        except Exception:
            # Ignore cleanup errors
            pass
