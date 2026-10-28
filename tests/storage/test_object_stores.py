"""The storage contract, on every backend: files, S3, Azure Blob Storage and Cloud Storage.

The cloud backends run against their emulators (``tests/emulators.py``), started here
when they are installed:
- `moto_server` (pip `moto[server]`), or the path in `HCP_TEST_MOTO_SERVER`;
- `azurite-blob` (npm `azurite`), or `HCP_TEST_AZURITE`;
- `fake-gcs-server`, or `HCP_TEST_FAKE_GCS`.

A backend whose emulator is missing is skipped. The emulators' logs and data go under
`test_artifacts/emulators/`.

Two things the emulators do not check, which the real services do, are noted where
they matter:
- whether S3 refuses content that does not match a grant's SHA-256;
- whether Cloud Storage honours a grant's create-only condition.
The agent's verification of size and SHA-256 is what the platform relies on either
way, and the tests check that it catches wrong content.
"""

from __future__ import annotations

import os
import uuid

import httpx
import pytest

from emulators import LOGS as EMULATORS
from emulators import binary, free_port, moto, start
from hydrocouple_platform.storage import ObjectExists, open_store, sha256_file

AZURITE_KEY = ("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/"
               "KBHBeksoGMGw==")  # Azurite's published development key


@pytest.fixture(scope="module")
def s3_uri(monkeypatch_module):
    process, port = moto()
    for k, v in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                 "AWS_DEFAULT_REGION": "us-east-1"}.items():
        monkeypatch_module.setenv(k, v)
    import boto3

    boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}").create_bucket(Bucket="hcp-store")
    yield f"s3://hcp-store/site-a?endpoint_url=http://127.0.0.1:{port}&region=us-east-1"
    process.kill()


@pytest.fixture(scope="module")
def azure_uri(monkeypatch_module):
    path = binary("HCP_TEST_AZURITE", "azurite-blob")
    if path is None:
        pytest.skip("azurite-blob is not installed (npm install azurite)")
    port = free_port()
    process = start("azurite", [path, "--blobHost", "127.0.0.1", "--blobPort", str(port),
                                 "--location", str(EMULATORS / "azurite"),
                                 "--skipApiVersionCheck", "--loose"], port)
    connection = (f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                  f"AccountKey={AZURITE_KEY};"
                  f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;")
    monkeypatch_module.setenv("AZURE_STORAGE_CONNECTION_STRING", connection)
    from azure.storage.blob import BlobServiceClient

    container = "hcp-" + uuid.uuid4().hex[:8]
    BlobServiceClient.from_connection_string(connection).create_container(container)
    yield f"azure://devstoreaccount1/{container}/site-a"
    process.kill()


@pytest.fixture(scope="module")
def gcs_uri(monkeypatch_module):
    path = binary("HCP_TEST_FAKE_GCS", "fake-gcs-server")
    if path is None:
        pytest.skip("fake-gcs-server is not installed")
    port = free_port()
    process = start("fake-gcs", [path, "-scheme", "http", "-host", "127.0.0.1",
                                  "-port", str(port), "-backend", "memory",
                                  "-public-host", f"127.0.0.1:{port}"], port)
    # A throwaway service-account key, so grants can be signed offline.
    import json

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode()
    keyfile = EMULATORS / "gcs-signer.json"
    keyfile.write_text(json.dumps({
        "type": "service_account", "project_id": "emulator", "private_key_id": "test",
        "private_key": pem, "client_email": "agent@emulator.iam.gserviceaccount.com",
        "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}))
    monkeypatch_module.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(keyfile))
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    storage.Client(project="emulator", credentials=AnonymousCredentials(),
                   client_options={"api_endpoint": f"http://127.0.0.1:{port}"}
                   ).create_bucket("hcp-store")
    yield f"gs://hcp-store/site-a?endpoint=http://127.0.0.1:{port}"
    process.kill()


@pytest.fixture(scope="module")
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(params=["file", "s3", "azure", "gs"])
def store(request, artifact_dir):
    if request.param == "file":
        return "file", open_store("file://" + str(artifact_dir / "store"))
    uri = request.getfixturevalue({"s3": "s3_uri", "azure": "azure_uri",
                                   "gs": "gcs_uri"}[request.param])
    # A fresh prefix per test keeps the emulators' state apart.
    base, _, query = uri.partition("?")
    uri = f"{base}/{uuid.uuid4().hex[:8]}" + (f"?{query}" if query else "")
    return request.param, open_store(uri)


def _files(artifact_dir):
    a, b = artifact_dir / "a.bin", artifact_dir / "b.bin"
    a.write_bytes(b"SWMM output block " * 4000)
    b.write_bytes(b"something else")
    return a, b


def test_objects_round_trip_with_their_size_and_sha256(store, artifact_dir):
    _, backend = store
    a, _ = _files(artifact_dir)
    backend.put_file("runs/r1/att1/model.out", a)
    assert backend.exists("runs/r1/att1/model.out")
    assert backend.stat("runs/r1/att1/model.out") == (a.stat().st_size, sha256_file(a))
    backend.get_file("runs/r1/att1/model.out", artifact_dir / "back" / "model.out")
    assert (artifact_dir / "back" / "model.out").read_bytes() == a.read_bytes()
    assert backend.stat("runs/r1/att1/missing") is None
    assert not backend.exists("runs/r1/att1/missing")
    backend.delete("runs/r1/att1/model.out")
    backend.delete("runs/r1/att1/model.out")  # deleting a missing object is not an error
    assert not backend.exists("runs/r1/att1/model.out")


def test_create_if_absent_never_replaces_an_object(store, artifact_dir):
    _, backend = store
    a, b = _files(artifact_dir)
    backend.put_file("runs/r1/att1/run.json", a, if_absent=True)
    with pytest.raises(ObjectExists):
        backend.put_file("runs/r1/att1/run.json", b, if_absent=True)
    assert backend.stat("runs/r1/att1/run.json")[1] == sha256_file(a)
    backend.put_file("runs/r1/att1/run.json", b)  # an unconditional write replaces it
    assert backend.stat("runs/r1/att1/run.json")[1] == sha256_file(b)


@pytest.mark.parametrize("key", ["../outside", "runs/../../outside", "/absolute", "a//b"])
def test_keys_cannot_escape_the_store(store, artifact_dir, key):
    _, backend = store
    a, _ = _files(artifact_dir)
    with pytest.raises(ValueError):
        backend.put_file(key, a)


def test_grants_upload_and_download_without_the_stores_credentials(store, artifact_dir):
    kind, backend = store
    if kind == "file":
        pytest.skip("file:// grants are paths, used by the local pool's clients directly")
    a, b = _files(artifact_dir)
    put = backend.grant("revisions/rev1/model/model.inp", "PUT", 300, sha256=sha256_file(a))
    response = httpx.put(put.url, content=a.read_bytes(), headers=put.headers)
    assert response.status_code in (200, 201), response.text
    assert backend.stat("revisions/rev1/model/model.inp") == (a.stat().st_size, sha256_file(a))
    get = backend.grant("revisions/rev1/model/model.inp", "GET", 300)
    assert httpx.get(get.url, headers=get.headers).content == a.read_bytes()
    again = httpx.put(put.url, content=b.read_bytes(), headers=put.headers)
    if kind != "gs":  # fake-gcs-server ignores x-goog-if-generation-match; Cloud Storage does not
        assert again.status_code in (409, 412)
        assert backend.stat("revisions/rev1/model/model.inp")[1] == sha256_file(a)


def test_content_that_does_not_match_its_grant_is_caught_by_verification(store, artifact_dir):
    kind, backend = store
    if kind == "file":
        pytest.skip("file:// grants are paths")
    a, b = _files(artifact_dir)
    put = backend.grant("revisions/rev1/x.dat", "PUT", 300, sha256=sha256_file(a))
    response = httpx.put(put.url, content=b.read_bytes(), headers=put.headers)
    # Amazon S3 refuses it outright; where a store (or emulator) accepts it, stat reports
    # the content that is really there, so the agent's verification refuses the revision.
    if response.status_code in (200, 201):
        assert backend.stat("revisions/rev1/x.dat") == (b.stat().st_size, sha256_file(b))


def test_large_s3_objects_are_written_in_parts_and_still_verified(s3_uri, artifact_dir,
                                                                   monkeypatch):
    from hydrocouple_platform.storage import s3

    monkeypatch.setattr(s3, "SINGLE_PUT_LIMIT", 6 * 1024**2)
    monkeypatch.setattr(s3, "PART_SIZE", 5 * 1024**2)  # S3's smallest part
    big = artifact_dir / "big.out"
    big.write_bytes(os.urandom(12 * 1024**2))
    base, _, query = s3_uri.partition("?")
    backend = open_store(f"{base}/parts?{query}")
    backend.put_file("runs/r1/att1/big.out", big, if_absent=True)
    assert backend.stat("runs/r1/att1/big.out") == (big.stat().st_size, sha256_file(big))
    with pytest.raises(ObjectExists):
        backend.put_file("runs/r1/att1/big.out", big, if_absent=True)


def test_deleting_a_prefix_removes_everything_under_it_and_nothing_else(store, artifact_dir):
    _, backend = store
    a, b = _files(artifact_dir)
    doomed = ["runs/o/p/r1/a1/model.out", "runs/o/p/r1/a1/run.json", "runs/o/p/r1/a2/x.json"]
    kept = ["runs/o/p/r10/a1/model.out", "revisions/rev1/model/model.inp"]
    for key in doomed + kept:
        backend.put_file(key, a)
    backend.put_file(doomed[0], b)  # a second version, where the store keeps versions
    assert backend.delete_prefix("runs/o/p/r1/") == 3
    assert not any(backend.exists(key) for key in doomed)
    assert all(backend.exists(key) for key in kept)  # a sibling run and a shared input
    assert backend.delete_prefix("runs/o/p/r1/") == 0
    for bad in ("runs/o/p/r1", "../x/", "runs//r1/", "/runs/o/p/r1/"):
        with pytest.raises(ValueError):
            backend.delete_prefix(bad)


def test_deleting_an_s3_prefix_removes_old_versions_too(s3_uri, artifact_dir):
    import boto3

    base, _, query = s3_uri.partition("?")
    endpoint = dict(p.split("=", 1) for p in query.split("&"))["endpoint_url"]
    client = boto3.client("s3", endpoint_url=endpoint)
    client.create_bucket(Bucket="hcp-versioned")
    client.put_bucket_versioning(Bucket="hcp-versioned",
                                 VersioningConfiguration={"Status": "Enabled"})
    backend = open_store(f"s3://hcp-versioned/site?{query}")
    a, b = _files(artifact_dir)
    backend.put_file("runs/o/p/r1/a1/model.out", a)
    backend.put_file("runs/o/p/r1/a1/model.out", b)  # two versions of one object
    assert backend.delete_prefix("runs/o/p/r1/") == 1
    left = client.list_object_versions(Bucket="hcp-versioned", Prefix="site/runs/o/p/r1/")
    assert not left.get("Versions") and not left.get("DeleteMarkers")
