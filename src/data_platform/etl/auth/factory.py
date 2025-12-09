from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

AuthProvider = TypeVar("AuthProvider", bound=object)


def creds_from_scope(
    cls: type[Any],
    *,
    secret_scope: str,
    secret_keys: Mapping[str, str],  # field_name -> secret_key
    dbutils: Any,  # dbutils or a stub with .secrets.get()
    strip_secrets: bool = False,
    **literals: Any,  # fixed kwargs (e.g. auth_url)
) -> Any:
    """Build *cls* pulling the secret-backed fields from Databricks Secrets.

    Only the names listed in *secret_keys* are fetched; everything else can be
    given as a literal or rely on the dataclass default.
    """
    kwargs = {name: dbutils.secrets.get(secret_scope, key) for name, key in secret_keys.items()}
    if strip_secrets:
        kwargs = {name: value.strip() for name, value in kwargs.items()}
    kwargs.update(literals)  # auth_url etc.
    # fill in remaining defaults automatically
    return cls(**kwargs)
