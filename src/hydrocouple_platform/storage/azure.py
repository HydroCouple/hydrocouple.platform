"""Azure Blob Storage: ``azure://account/container/prefix?endpoint=...``.

- **Credentials:** `DefaultAzureCredential` (workload identity in AKS, a managed
  identity, or the Azure CLI). With `AZURE_STORAGE_CONNECTION_STRING` set (the Azurite
  emulator, or a key a site chooses to use), that connection string is used instead.
- **Conditional writes:** `If-None-Match: *`.
- **Checksums:** Blob Storage keeps no SHA-256, so `stat` reads the object.
- **Grants:** user-delegation SAS URLs under a workload identity, account-key SAS
  under a connection string.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..contracts.agent import Grant
from . import ObjectExists, checked_prefix, object_key, sha256_stream, split_uri


class AzureBlobStore:
    def __init__(self, account: str, container: str, prefix: str = "",
                 endpoint: str | None = None, service=None):
        from azure.storage.blob import BlobServiceClient

        self.account, self.container, self.prefix = account, container, prefix
        connection = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if service is not None:
            self.service = service
        elif connection:
            self.service = BlobServiceClient.from_connection_string(connection)
        else:
            from azure.identity import DefaultAzureCredential

            self.service = BlobServiceClient(endpoint or f"https://{account}.blob.core.windows.net",
                                             credential=DefaultAzureCredential())
        self._container = self.service.get_container_client(container)

    @classmethod
    def from_uri(cls, uri: str) -> AzureBlobStore:
        (account, container), prefix, query = split_uri(uri, 2)
        return cls(account, container, prefix, endpoint=query.get("endpoint"))

    def _blob(self, key: str):
        return self._container.get_blob_client(object_key(self.prefix, key))

    def put_file(self, key: str, source: Path, *, if_absent: bool = False) -> None:
        from azure.core.exceptions import ResourceExistsError

        try:
            with open(source, "rb") as fh:
                self._blob(key).upload_blob(fh, overwrite=not if_absent)
        except ResourceExistsError:
            if if_absent:
                raise ObjectExists(key) from None
            raise

    def get_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with open(destination, "wb") as fh:
            self._blob(key).download_blob().readinto(fh)

    def stat(self, key: str) -> tuple[int, str] | None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            stream = self._blob(key).download_blob()
        except ResourceNotFoundError:
            return None
        return stream.properties.size, sha256_stream(stream.chunks())

    def exists(self, key: str) -> bool:
        return self._blob(key).exists()

    def delete(self, key: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._blob(key).delete_blob()
        except ResourceNotFoundError:
            pass

    def delete_prefix(self, prefix: str) -> int:
        """Every blob under the prefix, with its versions where the account keeps them.
        Soft-deleted blobs stay recoverable for the account's retention period."""
        from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

        name = self.prefix + checked_prefix(prefix)
        try:
            blobs = list(self._container.list_blobs(name_starts_with=name, include=["versions"]))
        except HttpResponseError:  # an account (or emulator) without versioning
            blobs = list(self._container.list_blobs(name_starts_with=name))
        count = 0
        for blob in blobs:
            current = getattr(blob, "is_current_version", None) is not False
            count += current
            try:
                if current:
                    self._container.delete_blob(blob.name, delete_snapshots="include")
                else:
                    self._container.delete_blob(blob.name, version_id=blob.version_id)
            except ResourceNotFoundError:
                pass
        return count

    def grant(self, key: str, method: str, expires_in: int,
              sha256: str | None = None) -> Grant:
        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        blob = self._blob(key)
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(seconds=expires_in)
        permission = BlobSasPermissions(create=True, write=True) if method == "PUT" else \
            BlobSasPermissions(read=True)
        common = dict(account_name=self.service.account_name, container_name=self.container,
                      blob_name=blob.blob_name, permission=permission, expiry=expiry)
        credential = self.service.credential
        account_key = getattr(credential, "account_key", None)
        if account_key:
            sas = generate_blob_sas(**common, account_key=account_key)
        else:
            delegation = self.service.get_user_delegation_key(now - timedelta(minutes=5),
                                                              expiry)
            sas = generate_blob_sas(**common, user_delegation_key=delegation)
        # A PUT grant creates a block blob and never replaces one.
        headers = {"x-ms-blob-type": "BlockBlob", "If-None-Match": "*"} if method == "PUT" \
            else {}
        return Grant(key=key, method=method, url=f"{blob.url}?{sas}", headers=headers,
                     expires_at=expiry)
