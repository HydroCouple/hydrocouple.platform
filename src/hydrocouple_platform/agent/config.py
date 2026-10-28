"""The execution agent's configuration file (``hcp agent --config agent.json``).

One JSON document names the control plane, the site's store, its egress policy, the
scheduler adapter and its settings, the pinned image keys, and where the runner
gateway listens. The target credential is never in it: it comes from
``HCP_AGENT_CREDENTIAL`` (a Kubernetes Secret, in a cluster install).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..contracts.jobs import EgressPolicy
from .adapters.kubernetes import KubernetesConfig
from .imagetrust import ImageTrust


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LocalPoolConfig(_Model):
    work_root: str
    slots: int = Field(default=1, ge=1)


class GatewayConfig(_Model):
    """Where the runner gateway listens, and the address runners use to reach it."""

    host: str = "127.0.0.1"
    port: int = Field(default=0, ge=0, le=65535)
    url: str | None = None  # a Service address on Kubernetes


class AgentConfig(_Model):
    control_plane: str
    store: str
    egress: EgressPolicy = EgressPolicy.SUMMARIES
    adapter: Literal["local-pool", "kubernetes"]
    local_pool: LocalPoolConfig | None = None
    kubernetes: KubernetesConfig | None = None
    image_trust: ImageTrust | None = None
    # Run images no pinned key signed: for development clusters only.
    allow_unsigned_images: bool = False
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    instance: str | None = None

    @model_validator(mode="after")
    def _adapter_settings(self) -> AgentConfig:
        section = {"local-pool": self.local_pool, "kubernetes": self.kubernetes}[self.adapter]
        if section is None:
            key = self.adapter.replace("-", "_")
            raise ValueError(f"adapter {self.adapter!r} needs its '{key}' settings")
        return self

    @classmethod
    def load(cls, path: str | Path) -> AgentConfig:
        return cls.model_validate(json.loads(Path(path).read_text(encoding="utf-8")))


def build_adapter(cfg: AgentConfig, kubeconfig: str | None = None):
    if cfg.adapter == "kubernetes":
        from .adapters.kubernetes import KubernetesAdapter, load_api_client

        return KubernetesAdapter(cfg.kubernetes, api_client=load_api_client(kubeconfig))
    from .adapters.local import LocalPoolAdapter

    return LocalPoolAdapter(cfg.local_pool.work_root, slots=cfg.local_pool.slots)


def build_agent(cfg: AgentConfig, credential: str, kubeconfig: str | None = None):
    from ..storage import open_store
    from .agent import Agent, AgentClient
    from .imagetrust import ImageVerifier

    images = ImageVerifier(cfg.image_trust) if cfg.image_trust else None
    return Agent(AgentClient(cfg.control_plane, credential), build_adapter(cfg, kubeconfig),
                 open_store(cfg.store), cfg.store, cfg.egress, instance=cfg.instance,
                 gateway_host=cfg.gateway.host, gateway_port=cfg.gateway.port,
                 gateway_url=cfg.gateway.url, images=images,
                 allow_unsigned_images=cfg.allow_unsigned_images)
