"""The capability record every scheduler adapter reports.

A flag set to ``None`` is unknown. An unknown capability rejects any request
that depends on it, so an adapter can never be asked for something it has not
declared it can do.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .jobs import JobClass, JobSpec


class AdapterCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    adapter: str
    family: Literal["local", "kubernetes", "batch", "serverless", "hpc"]
    job_classes: list[JobClass]
    max_task_seconds: int | None = Field(
        default=None, description="Hard runtime cap per task; None when the service has none."
    )
    has_runtime_cap: bool | None = Field(
        default=None,
        description="False when the service has no cap; None when it is unknown.",
    )
    max_parallel_tasks: int | None = None
    max_cpu: float | None = None
    max_memory_gib: float | None = None
    gpu: list[str] | None = None
    scratch_gib: float | None = None
    scratch_in_memory: bool | None = None
    interruption_tolerant: bool | None = None
    provider_retries: int | None = Field(
        default=None, description="Retries the service performs itself; must be 0."
    )
    idempotent_submit: bool | None = None
    status_integration: Literal["direct", "watch", "events", "polling"] | None = None
    submission_rate_per_second: float | None = None
    runs_images: bool = Field(
        default=True, description="False for the local pool, which runs the installed runner."
    )


def check_spec(cap: AdapterCapabilities, spec: JobSpec) -> list[str]:
    """Reasons ``spec`` cannot run on an adapter with ``cap``; empty when it can."""
    findings: list[str] = []
    res, limits = spec.resources, spec.limits

    if res.job_class not in cap.job_classes:
        findings.append(f"job class '{res.job_class.value}' is not supported by {cap.adapter}")
    if cap.provider_retries is None or cap.provider_retries != 0:
        findings.append(f"{cap.adapter} must report provider retries switched off")

    if cap.has_runtime_cap is None:
        findings.append(f"{cap.adapter} has not declared whether it caps task runtime")
    elif cap.has_runtime_cap and cap.max_task_seconds is not None:
        wanted = limits.estimated_runtime_seconds or limits.execution_timeout_seconds
        if wanted > cap.max_task_seconds:
            findings.append(
                f"runtime of {int(wanted)} s exceeds the {cap.max_task_seconds} s task cap "
                f"of {cap.adapter}"
            )

    _bounded(findings, cap.adapter, "CPU", res.cpu, cap.max_cpu)
    _bounded(findings, cap.adapter, "memory (GiB)", res.memory_gib, cap.max_memory_gib)
    _bounded(findings, cap.adapter, "scratch (GiB)", res.scratch_gib, cap.scratch_gib)

    parallel = res.pool_size if res.job_class == JobClass.EVALUATOR_POOL else res.ranks
    _bounded(findings, cap.adapter, "parallel tasks", parallel, cap.max_parallel_tasks)

    if res.gpu is not None:
        if cap.gpu is None:
            findings.append(f"{cap.adapter} has not declared GPU support")
        elif res.gpu not in cap.gpu:
            findings.append(f"GPU '{res.gpu}' is not available on {cap.adapter}")
    if spec.placement.interruption == "interruptible" and cap.interruption_tolerant is not True:
        findings.append(f"{cap.adapter} does not offer interruptible capacity")
    if spec.runtime.image is None and cap.runs_images:
        findings.append(
            f"workflow '{spec.workload.workflow}' has no qualified image for {cap.adapter}"
        )
    return findings


def _bounded(findings: list[str], adapter: str, what: str, wanted: float, cap: float | None):
    if cap is None:
        findings.append(f"{adapter} has not declared its {what} limit")
    elif wanted > cap:
        findings.append(f"requested {what} {wanted:g} exceeds {adapter} limit {cap:g}")
