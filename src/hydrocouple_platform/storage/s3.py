"""Amazon S3 and S3-compatible stores: ``s3://bucket/prefix?region=...&endpoint_url=...``.

- **Credentials:** boto3's default chain (IRSA or EKS Pod Identity in a pod, an
  instance profile, or environment variables for an S3-compatible store).
- **Conditional writes:** `If-None-Match: *`, so a create-if-absent can never replace
  an object.
- **Checksums:** every write sends a SHA-256, which Amazon S3 checks and keeps.
  `stat` reports that server-verified checksum for whole-object uploads on Amazon S3,
  and reads the object otherwise: multipart uploads keep only a composite checksum,
  and an S3-compatible store (an `endpoint_url`) may keep a client's checksum without
  checking it.
- **Upload grants:** presigned PUTs that carry the expected SHA-256, so S3 refuses
  any other content.
"""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..contracts.agent import Grant
from . import ObjectExists, object_key, sha256_file, sha256_stream, split_uri

SINGLE_PUT_LIMIT = 5 * 1024**3  # larger objects are uploaded in parts
PART_SIZE = 64 * 1024**2


def _b64(hex_digest: str) -> str:
    return base64.b64encode(bytes.fromhex(hex_digest)).decode()


def _precondition_failed(exc) -> bool:
    error = exc.response.get("Error", {})
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status == 412 or error.get("Code") in ("PreconditionFailed",
                                                  "ConditionalRequestConflict")


class S3Store:
    def __init__(self, bucket: str, prefix: str = "", region: str | None = None,
                 endpoint_url: str | None = None, client=None):
        import boto3
        from botocore.config import Config

        self.bucket, self.prefix = bucket, prefix
        self.trust_checksums = endpoint_url is None  # Amazon S3 verifies what it keeps
        self.client = client or boto3.client(
            "s3", region_name=region, endpoint_url=endpoint_url,
            config=Config(signature_version="s3v4",
                          s3={"addressing_style": "path" if endpoint_url else "auto"}))

    @classmethod
    def from_uri(cls, uri: str) -> S3Store:
        (bucket,), prefix, query = split_uri(uri, 1)
        return cls(bucket, prefix, region=query.get("region"),
                   endpoint_url=query.get("endpoint_url"))

    def _key(self, key: str) -> str:
        return object_key(self.prefix, key)

    def put_file(self, key: str, source: Path, *, if_absent: bool = False) -> None:
        from botocore.exceptions import ClientError

        name, digest = self._key(key), sha256_file(source)
        condition = {"IfNoneMatch": "*"} if if_absent else {}
        try:
            if source.stat().st_size <= SINGLE_PUT_LIMIT:
                with open(source, "rb") as fh:
                    self.client.put_object(Bucket=self.bucket, Key=name, Body=fh,
                                           ChecksumSHA256=_b64(digest), **condition)
            else:
                self._put_parts(name, source, condition)
        except ClientError as exc:
            if if_absent and _precondition_failed(exc):
                raise ObjectExists(key) from None
            raise

    def _put_parts(self, name: str, source: Path, condition: dict) -> None:
        import hashlib

        upload = self.client.create_multipart_upload(Bucket=self.bucket, Key=name,
                                                     ChecksumAlgorithm="SHA256")
        parts = []
        try:
            with open(source, "rb") as fh:
                for number, block in enumerate(iter(lambda: fh.read(PART_SIZE), b""), 1):
                    checksum = base64.b64encode(hashlib.sha256(block).digest()).decode()
                    done = self.client.upload_part(
                        Bucket=self.bucket, Key=name, UploadId=upload["UploadId"],
                        PartNumber=number, Body=block, ChecksumSHA256=checksum)
                    parts.append({"PartNumber": number, "ETag": done["ETag"],
                                  "ChecksumSHA256": checksum})
            self.client.complete_multipart_upload(
                Bucket=self.bucket, Key=name, UploadId=upload["UploadId"],
                MultipartUpload={"Parts": parts}, **condition)
        except Exception:
            self.client.abort_multipart_upload(Bucket=self.bucket, Key=name,
                                               UploadId=upload["UploadId"])
            raise

    def get_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, self._key(key), str(destination))

    def _head(self, key: str) -> dict | None:
        from botocore.exceptions import ClientError

        try:
            return self.client.head_object(Bucket=self.bucket, Key=self._key(key),
                                           ChecksumMode="ENABLED")
        except ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return None
            raise

    def stat(self, key: str) -> tuple[int, str] | None:
        head = self._head(key)
        if head is None:
            return None
        checksum = head.get("ChecksumSHA256")
        # A whole-object checksum is the SHA-256 S3 verified on upload; a multipart one
        # ("...-N") is a checksum of part checksums, so read the object instead.
        if (self.trust_checksums and checksum and "-" not in checksum
                and head.get("ChecksumType") != "COMPOSITE"):
            return head["ContentLength"], base64.b64decode(checksum).hex()
        body = self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"]
        return head["ContentLength"], sha256_stream(body.iter_chunks(1 << 20))

    def exists(self, key: str) -> bool:
        return self._head(key) is not None

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(key))

    def grant(self, key: str, method: str, expires_in: int,
              sha256: str | None = None) -> Grant:
        params = {"Bucket": self.bucket, "Key": self._key(key)}
        headers: dict[str, str] = {}
        if method == "PUT":
            # A PUT grant never replaces an object, and is bound to its content when known.
            params["IfNoneMatch"] = headers["If-None-Match"] = "*"
            if sha256:
                params["ChecksumSHA256"] = headers["x-amz-checksum-sha256"] = _b64(sha256)
            url = self.client.generate_presigned_url("put_object", Params=params,
                                                     ExpiresIn=expires_in)
        else:
            url = self.client.generate_presigned_url("get_object", Params=params,
                                                     ExpiresIn=expires_in)
        return Grant(key=key, method=method, url=url, headers=headers,
                     expires_at=datetime.now(timezone.utc) + timedelta(seconds=expires_in))
