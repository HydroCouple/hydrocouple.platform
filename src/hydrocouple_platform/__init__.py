"""HydroCouple execution platform: durable, bring-your-own-compute execution.

Subpackages:

- ``contracts``: versioned data contracts shared by every process.
- ``db``, ``services``, ``api``: the control plane.
- ``agent``: the customer-run execution agent and its scheduler adapters.
- ``runner``: approved workflow handlers that run next to the engine.
- ``packaging``: builds immutable composition revisions from local files.
- ``client``: the HTTP client used by the GUI, CLI and MCP adapters.
"""

__version__ = "0.1.0.dev0"
