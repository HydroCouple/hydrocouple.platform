"""The registry of approved, versioned workflows.

A user or agent chooses a workflow by name and version and supplies parameters
that are validated against its schema. Parameters can never select an image,
command, executable path, component library or credential.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import jsonschema

from .contracts.errors import ErrorCode, PlatformError
from .contracts.jobs import JobClass

OPENSWMM_ENGINE_COMPONENT = "org.hydrocouple.openswmm.engine"


@dataclass(frozen=True)
class Workflow:
    name: str
    version: str
    job_class: JobClass
    handler: str
    allowed_components: frozenset[str]
    parameters_schema: dict[str, Any]
    required_artifacts: tuple[str, ...]
    default_profile: str = "cpu-small"
    # Signed image digest per architecture; empty until an image is qualified.
    images: dict[str, str] = field(default_factory=dict)
    description: str = ""

    def validate_parameters(self, parameters: dict[str, Any]) -> dict[str, Any]:
        try:
            jsonschema.validate(parameters, self.parameters_schema)
        except jsonschema.ValidationError as exc:
            raise PlatformError(
                ErrorCode.VALIDATION_ERROR,
                f"parameters for {self.name}@{self.version} are invalid: {exc.message}",
                next_action="Call platform_targets to read the workflow's parameter schema.",
            ) from None
        resolved = dict(parameters)
        for key, spec in self.parameters_schema.get("properties", {}).items():
            if key not in resolved and "default" in spec:
                resolved[key] = spec["default"]
        return resolved


SWMM_SIMULATION_V1 = Workflow(
    name="swmm.simulation",
    version="1",
    job_class=JobClass.SINGLE,
    handler="swmm.simulation",
    allowed_components=frozenset({OPENSWMM_ENGINE_COMPONENT}),
    parameters_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "threads": {"type": "integer", "minimum": 1, "maximum": 64, "default": 1},
            "progress_interval_seconds": {
                "type": "number", "minimum": 0.2, "maximum": 300, "default": 2.0,
            },
        },
    },
    required_artifacts=("run.json", "attempt.json", "model.rpt", "model.out"),
    description="Run one SWMM model to completion with the OpenSWMM engine.",
)

GYM_EVALUATE_V1 = Workflow(
    name="gym.evaluate",
    version="1",
    job_class=JobClass.EVALUATOR_POOL,
    handler="gym.evaluate",
    allowed_components=frozenset({OPENSWMM_ENGINE_COMPONENT}),
    parameters_schema={
        "type": "object",
        "additionalProperties": False,
        "required": ["env_config"],
        "properties": {
            # An openswmm_gymnasium EnvConfig; inp_path is the model's path in the revision.
            "env_config": {"type": "object"},
            "max_steps_per_episode": {"type": "integer", "minimum": 1, "default": 10_000},
            "threads": {"type": "integer", "minimum": 1, "maximum": 64, "default": 1},
            # Engine processes per task; each keeps its env loaded across evaluations.
            "workers": {"type": "integer", "minimum": 1, "maximum": 32, "default": 1},
            "evaluation_timeout_seconds": {
                "type": "number", "minimum": 1, "maximum": 86_400, "default": 600,
            },
            "idle_timeout_seconds": {
                "type": "number", "minimum": 10, "maximum": 86_400, "default": 600,
            },
        },
    },
    required_artifacts=("run.json", "attempt.json"),
    description="Evaluate study candidates on an openswmm_gymnasium env: one episode per "
                "candidate, the engine kept loaded across evaluations.",
)

REGISTRY: dict[tuple[str, str], Workflow] = {
    (SWMM_SIMULATION_V1.name, SWMM_SIMULATION_V1.version): SWMM_SIMULATION_V1,
    (GYM_EVALUATE_V1.name, GYM_EVALUATE_V1.version): GYM_EVALUATE_V1,
}


def get_workflow(name: str, version: str) -> Workflow:
    try:
        return REGISTRY[(name, version)]
    except KeyError:
        known = ", ".join(f"{n}@{v}" for n, v in sorted(REGISTRY))
        raise PlatformError(
            ErrorCode.VALIDATION_ERROR,
            f"workflow {name}@{version} is not approved; approved workflows: {known}",
        ) from None
