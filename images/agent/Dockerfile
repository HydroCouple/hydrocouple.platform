# The execution agent image, for agents that run inside a cluster: the hydrocouple_platform
# agent with the Kubernetes adapter, the S3, Azure Blob Storage and Cloud Storage backends,
# and cosign for verifying runner images. images/build.sh prepares the wheelhouse and the
# checksum-verified cosign binary. BASE must be pinned by digest and provide Python 3.10 or
# later with venv and useradd.
ARG BASE
FROM ${BASE}
COPY wheelhouse/ /tmp/wheelhouse/
COPY --chmod=0755 cosign /usr/local/bin/cosign
RUN python3 -m venv /opt/hydrocouple \
 && /opt/hydrocouple/bin/pip install --no-cache-dir --no-index --find-links /tmp/wheelhouse \
      "hydrocouple.platform[agent,s3,azure,gcs]" \
 && rm -rf /tmp/wheelhouse \
 && /opt/hydrocouple/bin/python -c "import hydrocouple_platform.agent.config, kubernetes" \
 && cosign version >/dev/null
RUN groupadd -g 10001 hcp && useradd -u 10001 -g 10001 -M -d /tmp -s /usr/sbin/nologin hcp
# The agent pod runs with a read-only root filesystem; cosign keeps its cache in HOME.
ENV PATH=/opt/hydrocouple/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp
USER 10001:10001
ENTRYPOINT ["hcp"]
CMD ["agent", "--config", "/etc/hydrocouple/config/agent.json"]
LABEL org.opencontainers.image.source="https://github.com/HydroCouple/hydrocouple.platform" \
      org.opencontainers.image.title="hydrocouple-agent"
