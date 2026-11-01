"""The capability record every scheduler adapter reports.

A flag set to ``None`` is unknown. An unknown capability rejects any request
that depends on it, so an adapter can never be asked for something it has not
declared it can do.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .jobs import JobClass, JobSpec, Resources


class TaskSize(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu: float = Field(gt=0)
    memory_gib: float = Field(gt=0)


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
    scratch_in_memory: bool | None = Field(
        default=None, description="True when writes to scratch use the task's memory."
    )
    task_sizes: list[TaskSize] | None = Field(
        default=None, min_length=1,
        description="The CPU and memory pairs the service offers, when it offers only some "
        "(Fargate). A request runs on the smallest pair with enough of both. None when any "
        "combination up to max_cpu and max_memory_gib runs.",
    )
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

    @model_validator(mode="after")
    def _sizes_within_maxima(self) -> AdapterCapabilities:
        for size in self.task_sizes or []:
            if (self.max_cpu is not None and size.cpu > self.max_cpu) or (
                    self.max_memory_gib is not None and size.memory_gib > self.max_memory_gib):
                raise ValueError(f"task size {size.cpu:g} CPU, {size.memory_gib:g} GiB is beyond "
                                 "max_cpu or max_memory_gib")
        return self


def task_memory_gib(cap: AdapterCapabilities, res: Resources) -> float:
    """The memory a request needs on this service: with its scratch, where scratch is memory
    (plan §4, serverless rule 2)."""
    return res.memory_gib + (res.scratch_gib if cap.scratch_in_memory else 0)


def fit_task_size(cap: AdapterCapabilities, res: Resources) -> TaskSize | None:
    """The smallest of the service's sizes with enough CPU and memory for a request; None
    when none has, or the service runs any combination (``task_sizes`` is None)."""
    memory = task_memory_gib(cap, res)
    fits = [s for s in cap.task_sizes or [] if s.cpu >= res.cpu and s.memory_gib >= memory]
    return min(fits, key=lambda s: (s.cpu, s.memory_gib)) if fits else None


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
    elif cap.has_runtime_cap and cap.max_task_seconds is None:
        findings.append(f"{cap.adapter} caps task runtime but has not declared how long")
    elif cap.has_runtime_cap:
        wanted = limits.estimated_runtime_seconds or limits.execution_timeout_seconds
        if wanted > cap.max_task_seconds:
            findings.append(
                f"runtime of {int(wanted)} s exceeds the {cap.max_task_seconds} s task cap "
                f"of {cap.adapter}"
            )

    memory, memory_note = task_memory_gib(cap, res), ""
    if cap.scratch_in_memory:
        memory_note = f" ({res.memory_gib:g} and {res.scratch_gib:g} GiB of scratch held in memory)"
    elif cap.scratch_in_memory is None and res.scratch_gib > 0:
        findings.append(f"{cap.adapter} has not declared whether scratch is held in memory")
    _bounded(findings, cap.adapter, "CPU", res.cpu, cap.max_cpu)
    _bounded(findings, cap.adapter, "memory (GiB)", memory, cap.max_memory_gib, memory_note)
    if cap.scratch_in_memory is False:
        _bounded(findings, cap.adapter, "scratch (GiB)", res.scratch_gib, cap.scratch_gib)
    if cap.task_sizes is not None and fit_task_size(cap, res) is None:
        findings.append(f"no {cap.adapter} task size has {res.cpu:g} CPU and {memory:g} GiB of "
                        f"memory; its sizes go up to {max(s.cpu for s in cap.task_sizes):g} CPU "
                        f"and up to {max(s.memory_gib for s in cap.task_sizes):g} GiB")

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


def _bounded(findings: list[str], adapter: str, what: str, wanted: float, cap: float | None,
             note: str = ""):
    if cap is None:
        findings.append(f"{adapter} has not declared its {what} limit")
    elif wanted > cap:
        findings.append(f"requested {what} {wanted:g}{note} exceeds {adapter} limit {cap:g}")
