# The runner image for container targets: the hydrocouple_platform runner with the
# OpenSWMM engine, the openswmm_gymnasium evaluator and the S3, Azure Blob Storage and
# Cloud Storage backends, installed from the wheelhouse
# that images/build.sh prepares. BASE must be pinned by digest and provide
# Python 3.10 or later with venv, useradd, and the libraries the engine wheel links
# (libgomp; libsqlite3 for an engine wheel not repaired with auditwheel). The official
# python:3.10-slim-bookworm image with libgomp1 added is one such base
# (deploy/gate/README.md).
ARG BASE
FROM ${BASE}
COPY wheelhouse/ /tmp/wheelhouse/
RUN python3 -m venv /opt/hydrocouple \
 && /opt/hydrocouple/bin/pip install --no-cache-dir --no-index --find-links /tmp/wheelhouse \
      "hydrocouple.platform[runner,gym,s3,azure,gcs]" \
 && rm -rf /tmp/wheelhouse \
 && /opt/hydrocouple/bin/python -c "import hydrocouple_platform.runner.gym, openswmm.engine, openswmm_gymnasium.optimize"
# Runner pods run as this user, with a read-only root filesystem and /work as an emptyDir.
RUN groupadd -g 10001 hcp && useradd -u 10001 -g 10001 -M -d /work -s /usr/sbin/nologin hcp \
 && mkdir -p /work && chown 10001:10001 /work
ENV PATH=/opt/hydrocouple/bin:$PATH PYTHONDONTWRITEBYTECODE=1 HOME=/work
USER 10001:10001
WORKDIR /work
LABEL org.opencontainers.image.source="https://github.com/HydroCouple/hydrocouple.platform" \
      org.opencontainers.image.title="hydrocouple-runner"
