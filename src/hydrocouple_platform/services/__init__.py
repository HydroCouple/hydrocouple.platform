"""Shared application services: the single place where authorization, budgets,
lifecycle rules and publication are enforced. The HTTP API and the hosted MCP
toolset both call these, in process or through ``PlatformClient``.
"""

from __future__ import annotations

from .agentops import AgentServices
from .auth import Principal, UserTokenVerifier, VerifiedClaims
from .catalog import CatalogServices
from .revisions import RevisionServices
from .runs import RunServices
from .studies import StudyServices


class PlatformServices(CatalogServices, RevisionServices, RunServices, StudyServices,
                       AgentServices):
    """All platform operations behind one interface."""


__all__ = ["PlatformServices", "Principal", "UserTokenVerifier", "VerifiedClaims"]
