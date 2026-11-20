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
    # Whether a run may continue from the saved state of an earlier run of this workflow.
    resumable: bool = False

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

SWMM_SIMULATION_V2 = Workflow(
    name="swmm.simulation",
    version="2",
    job_class=JobClass.SINGLE,
    handler="swmm.simulation",
    allowed_components=frozenset({OPENSWMM_ENGINE_COMPONENT}),
    parameters_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            **SWMM_SIMULATION_V1.parameters_schema["properties"],
            # A scenario: element field values written after the model opens and before it
            # initializes. The runner refuses a path the engine catalog does not list as a
            # writable float, int or bool element field.
            "overrides": {
                "type": "array",
                "maxItems": 10_000,
                "default": [],
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["path", "id", "value"],
                    "properties": {
                        "path": {"type": "string", "pattern": r"^[a-z0-9_]+(\.[a-z0-9_]+)+$"},
                        "id": {"type": "string", "minLength": 1},
                        "value": {"type": ["number", "boolean"]},
                    },
                },
            },
        },
    },
    required_artifacts=SWMM_SIMULATION_V1.required_artifacts,
    description="Run one SWMM model to completion with the OpenSWMM engine, with element "
                "field overrides (a scenario of the revision).",
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

# Model times are the engine's UTC wall clock, written without a zone.
_WALL_CLOCK = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$"
_ELEMENT_FIELD = r"^[a-z0-9_]+(\.[a-z0-9_]+)+$"

TWIN_CYCLE_V1 = Workflow(
    name="twin.cycle",
    version="1",
    job_class=JobClass.SINGLE,
    handler="twin.cycle",
    allowed_components=frozenset({OPENSWMM_ENGINE_COMPONENT}),
    parameters_schema={
        "type": "object",
        "additionalProperties": False,
        "required": ["cycle_end"],
        "properties": {
            "threads": {"type": "integer", "minimum": 1, "maximum": 64, "default": 1},
            # The cycle runs to here and records its state for the next cycle.
            "cycle_end": {"type": "string", "pattern": _WALL_CLOCK},
            # Rain telemetry: each gage reads a column of a CSV of the revision, cut before
            # the cycle's end.
            "telemetry": {
                "type": "array",
                "maxItems": 1_000,
                "default": [],
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["gage", "series"],
                    "properties": {
                        "gage": {"type": "string", "minLength": 1},
                        "series": {"type": "string", "minLength": 1},
                        "column": {"type": "string", "minLength": 1},
                        "format": {"enum": ["INTENSITY", "VOLUME"], "default": "INTENSITY"},
                    },
                },
            },
            # Setpoints a person approved (the control plane checks each proposal), applied
            # from their effective times.
            "setpoints": {
                "type": "array",
                "maxItems": 10_000,
                "default": [],
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["link", "value", "effective_from", "proposal_id"],
                    "properties": {
                        "link": {"type": "string", "minLength": 1},
                        "value": {"type": "number", "minimum": 0, "maximum": 1},
                        "effective_from": {"type": "string", "pattern": _WALL_CLOCK},
                        "proposal_id": {"type": "string", "minLength": 1},
                    },
                },
            },
            # A forecast from the cycle's end state, with no rain after the cycle's end.
            "forecast": {
                "type": "object",
                "additionalProperties": False,
                "required": ["horizon_minutes"],
                "properties": {
                    "horizon_minutes": {"type": "number", "exclusiveMinimum": 0,
                                        "maximum": 10_080},
                    "every_minutes": {"type": "number", "exclusiveMinimum": 0, "default": 5},
                    "observe": {
                        "type": "array",
                        "maxItems": 1_000,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["path", "id"],
                            "properties": {
                                "path": {"type": "string", "pattern": _ELEMENT_FIELD},
                                "id": {"type": "string", "minLength": 1},
                            },
                        },
                    },
                },
            },
            # A control curve over the forecast: the link's setting, linear in an observed
            # field between knots, decided at the issue time and each sample time and rounded
            # to the resolution; each change becomes a setpoint proposal.
            "policy": {
                "type": "object",
                "additionalProperties": False,
                "required": ["link", "observe", "element", "x", "setting"],
                "properties": {
                    "link": {"type": "string", "minLength": 1},
                    "observe": {"type": "string", "pattern": _ELEMENT_FIELD},
                    "element": {"type": "string", "minLength": 1},
                    "x": {"type": "array", "minItems": 2, "items": {"type": "number"}},
                    "setting": {"type": "array", "minItems": 2,
                                "items": {"type": "number", "minimum": 0, "maximum": 1}},
                    "resolution": {"type": "number", "exclusiveMinimum": 0, "maximum": 1,
                                   "default": 0.05},
                },
            },
        },
    },
    required_artifacts=("run.json", "attempt.json", "model.rpt", "state.h5", "cycle.json"),
    description="One digital twin cycle on the OpenSWMM engine: continue from the previous "
                "cycle's state with the telemetry that arrived, record the state at the cycle's "
                "end, then forecast and propose setpoints.",
    resumable=True,
)

REGISTRY: dict[tuple[str, str], Workflow] = {
    (SWMM_SIMULATION_V1.name, SWMM_SIMULATION_V1.version): SWMM_SIMULATION_V1,
    (SWMM_SIMULATION_V2.name, SWMM_SIMULATION_V2.version): SWMM_SIMULATION_V2,
    (GYM_EVALUATE_V1.name, GYM_EVALUATE_V1.version): GYM_EVALUATE_V1,
    (TWIN_CYCLE_V1.name, TWIN_CYCLE_V1.version): TWIN_CYCLE_V1,
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
