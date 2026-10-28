"""Google Cloud Storage: ``gs://bucket/prefix?endpoint=...``.

- **Credentials:** Application Default Credentials (workload identity federation for
  GKE, a service account elsewhere). `endpoint` points at an emulator, which is used
  anonymously; its grants are signed with the key in `GOOGLE_APPLICATION_CREDENTIALS`.
- **Conditional writes:** `ifGenerationMatch=0`.
- **Checksums:** Cloud Storage keeps MD5 and CRC32C but no SHA-256, so `stat` reads
  the object.
- **Grants:** V4 signed URLs. Under workload identity they are signed through the IAM
  Credentials API, which needs `roles/iam.serviceAccountTokenCreator` on the agent's
  own service account.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..contracts.agent import Grant
from . import ObjectExists, object_key, sha256_stream, split_uri


class GCSStore:
    def __init__(self, bucket: str, prefix: str = "", endpoint: str | None = None,
                 client=None):
        from google.cloud import storage

        self.endpoint, self.prefix = endpoint, prefix
        if client is None and endpoint:
            from google.auth.credentials import AnonymousCredentials

            client = storage.Client(project="emulator", credentials=AnonymousCredentials(),
                                    client_options={"api_endpoint": endpoint})
        self.client = client or storage.Client()
        self.bucket = self.client.bucket(bucket)

    @classmethod
    def from_uri(cls, uri: str) -> GCSStore:
        (bucket,), prefix, query = split_uri(uri, 1)
        return cls(bucket, prefix, endpoint=query.get("endpoint"))

    def _blob(self, key: str):
        return self.bucket.blob(object_key(self.prefix, key))

    def put_file(self, key: str, source: Path, *, if_absent: bool = False) -> None:
        from google.api_core.exceptions import PreconditionFailed

        try:
            self._blob(key).upload_from_filename(
                str(source), if_generation_match=0 if if_absent else None)
        except PreconditionFailed:
            raise ObjectExists(key) from None

    def get_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._blob(key).download_to_filename(str(destination))

    def stat(self, key: str) -> tuple[int, str] | None:
        blob = self.bucket.get_blob(object_key(self.prefix, key))
        if blob is None:
            return None
        # Read the generation the size came from, even if the object is replaced meanwhile.
        with blob.open("rb", chunk_size=1 << 20, if_generation_match=blob.generation) as fh:
            digest = sha256_stream(iter(lambda: fh.read(1 << 20), b""))
        return blob.size, digest

    def exists(self, key: str) -> bool:
        return self._blob(key).exists()

    def delete(self, key: str) -> None:
        from google.api_core.exceptions import NotFound

        try:
            self._blob(key).delete()
        except NotFound:
            pass

    def grant(self, key: str, method: str, expires_in: int,
              sha256: str | None = None) -> Grant:
        blob = self._blob(key)
        # A PUT grant never replaces an object; the header is part of the signature.
        headers = {"x-goog-if-generation-match": "0"} if method == "PUT" else {}
        options: dict = {}
        credentials = self.client._credentials
        if self.endpoint:
            options["api_access_endpoint"] = self.endpoint
            keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if keyfile:
                from google.oauth2 import service_account

                options["credentials"] = service_account.Credentials.from_service_account_file(
                    keyfile)
        elif not hasattr(credentials, "sign_bytes"):  # workload identity: sign through IAM
            from google.auth.transport.requests import Request

            credentials.refresh(Request())
            options = {"service_account_email": credentials.service_account_email,
                       "access_token": credentials.token}
        url = blob.generate_signed_url(version="v4", expiration=timedelta(seconds=expires_in),
                                       method=method, headers=headers or None, **options)
        return Grant(key=key, method=method, url=url, headers=headers,
                     expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in))
