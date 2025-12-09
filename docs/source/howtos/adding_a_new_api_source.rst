==================================================
How to ingest a **new API source** on Data-Platform
==================================================

This guide targets engineers working on the ETL layer.
It assumes you already know Databricks notebooks, the
``TaskContext`` helper and the credential utilities
(``creds_from_scope``, ``ApiKeyAuth``, ``PasswordGrantOauth`` …).

Overview
--------

Modern readers build upon three reusable layers:

+-------------------------------------------------------+-------------------------------------------------------------+
| **Layer**                                             | **Key classes / mix-ins**                                   |
+=======================================================+=============================================================+
| HTTP core – connection pool, retries, auth, chunks,   |                                                             |
| lookback, lookahead,  save to volume                  | ``AsyncHttpApiClient`` / ``SyncHttpApiClient``              |
+-------------------------------------------------------+-------------------------------------------------------------+
| Endpoint switching (one client → many endpoints)      | ``EndpointDescriptor`` + ``EndpointExecutorMixin``          |
+-------------------------------------------------------+-------------------------------------------------------------+
| Incremental cursor (inject + auto-commit)             | ``_HttpCursorAwareMixin`` →                                 |
|                                                       | ``IncrementalSyncHttpApiClient`` /                          |
|                                                       | ``IncrementalAsyncHttpApiClient``                           |
+-------------------------------------------------------+-------------------------------------------------------------+

Decision tree
-------------

1. *Single endpoint and blocking is OK?* → subclass **``SyncHttpApiClient``**.
2. *Many requests / needs parallel I/O?* → subclass **``AsyncHttpApiClient``**.
3. *One client must talk to several endpoints?*
   → add **``EndpointExecutorMixin``** + one ``EndpointDescriptor`` per endpoint.
4.  WIP - not ready - *Need change-data capture?* → start from
   ``IncrementalSyncHttpApiClient`` or ``IncrementalAsyncHttpApiClient``;
   provide a ``cursor`` object.

Quick start – simple one-endpoint reader
----------------------------------------

.. code-block:: python

   from dataclasses import dataclass
   from data_platform.etl.extract.http.http_sync_client import SyncHttpApiClient

   @dataclass(slots=True, kw_only=True)
   class MySourceReader(SyncHttpApiClient):
       endpoint_url: str = "https://api.example.com/data"

       def build_param_chunks(self) -> list[dict]:
           # ten pages → ten requests
           return [{"page": i} for i in range(1, 11)]

       def handle_response(self, data, *, chunk=None):
           return data["items"]

       def get_file_name(self, chunk):
           return f"/page={chunk['page']}/data.json"

Notebook skeleton
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from databricks.sdk.runtime import dbutils
   from data_platform.etl.core.task_context import TaskContext
   from data_platform.etl.extract.core.provider import ApiKeyAuth, creds_from_scope
   from data_platform.data_model import FileVolumeIdentifier
   from my_source_reader import MySourceReader

   ctx = TaskContext(catalog_prefix="bronze_", schema_prefix="mysource_")

   creds = creds_from_scope(
       cls=ApiKeyAuth,
       secret_scope="bronze_secretscope",
       secret_keys={"api_key": "my-api-key"},
       dbutils=dbutils,
   )

   MySourceReader(
       context=ctx,
       credentials=creds,
       file_volume=FileVolumeIdentifier("bronze", "mysource", "data"),
   ).execute()

Unified client for many endpoints
---------------------------------

.. code-block:: python

   from __future__ import annotations
   from dataclasses import dataclass, field
   from datetime import timedelta
   from typing import Any

   from data_platform.etl.extract.http2.http_async_client import AsyncHttpApiClient
   from data_platform.etl.extract.http2.endpoint import EndpointDescriptor, EndpointExecutorMixin

   @dataclass(slots=True, kw_only=True)
   class MyApiClient(EndpointExecutorMixin, AsyncHttpApiClient):
       file_volume: Any
       lookahead: timedelta = timedelta(days=1)
       _endpoints: dict[str, EndpointDescriptor] = field(init=False, default_factory=dict)

       def __post_init__(self) -> None:
           self.headers = {**(self.headers or {}),
                           "Authorization": f"Bearer {self.credentials.token}"}
           super().__post_init__()
           self._register_endpoints()

       # ── registry ──────────────────────────────────────────────────
       def _register_endpoints(self) -> None:
           make_chunks = lambda self=self: [{"id": i} for i in range(10)]
           self._endpoints = {
               "foo": EndpointDescriptor(
                   url="https://api.example.com/foo",
                   build_param_chunks=make_chunks,
                   build_params=lambda c: {"id": c["id"]},
                   get_file_name=lambda c: f"/foo/{c['id']}.json",
               ),
               "bar": EndpointDescriptor(
                   url="https://api.example.com/bar",
                   overrides={"lookahead": timedelta(days=7)},
                   build_param_chunks=make_chunks,  # reuse same chunks
               ),
           }

       # ── public helpers ────────────────────────────────────────────
       def get_foo(self, **ovr): return self._execute_endpoint("foo", **ovr)
       def get_bar(self, **ovr): return self._execute_endpoint("bar", **ovr)

Incremental reader template
---------------------------
This is not ready yet, it's work in progress. Don't use it.

.. code-block:: python

   from dataclasses import dataclass
   from data_platform.etl.extract.cursor import TimestampCursor
   from data_platform.etl.extract.http2.incremental import IncrementalAsyncHttpApiClient

   CURSOR_BACKEND = FileBackend("/mnt/state/my_source_cursor.json")

   @dataclass(slots=True, kw_only=True)
   class MyIncReader(IncrementalAsyncHttpApiClient):
       cursor = TimestampCursor(
           field_name="updated_at",
           backend=CURSOR_BACKEND,
           state_key="my_source_stream",
           lookback=timedelta(minutes=5),
       )
       endpoint_url = "https://api.foo.com/v1/records"

Testing checklist
-----------------

* ``build_param_chunks`` renders the expected items.
* Filenames partition by date / id as intended.
* Credential scope & secret name are correct.
* Concurrency knobs respect the provider’s rate-limits.
* Dry-run on a narrow window; confirm files appear in the bronze volume.

Reference implementations
-------------------------

* **NordpoolApiClient** – subclasses of async with poll-on-empty.
* **MontelApiClient** – multi-endpoint async + synchronous metadata reader.
* **JaoClient** – async with auctions & bids.
* **MontelMetadataApiClient** – minimal synchronous reader.

Use these blueprints instead of starting from scratch.
