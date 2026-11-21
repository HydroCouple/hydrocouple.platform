# Versioned contracts

Download the JSON Schemas shipped with this source revision. The base client and the agent use these contracts across the control-plane boundary.

| Schema | Use |
| --- | --- |
| {download}`run-request-1.json <../src/hydrocouple_platform/schemas/platform/run-request-1.json>` | Project, revision, target and workflow supplied by the client. |
| {download}`run-accepted-1.json <../src/hydrocouple_platform/schemas/platform/run-accepted-1.json>` | Run and job identifiers returned to the client. |
| {download}`job-spec-1.json <../src/hydrocouple_platform/schemas/platform/job-spec-1.json>` | Runnable work and input references passed to the target. |
| {download}`agent-poll-response-1.json <../src/hydrocouple_platform/schemas/platform/agent-poll-response-1.json>` | Leases, cancellation requests and agent work. |
| {download}`event-1.json <../src/hydrocouple_platform/schemas/platform/event-1.json>` | Progress and diagnostic events for one attempt. |
| {download}`revision-manifest-1.json <../src/hydrocouple_platform/schemas/platform/revision-manifest-1.json>` | Input bundle and file identities. |
| {download}`attempt-manifest-1.json <../src/hydrocouple_platform/schemas/platform/attempt-manifest-1.json>` | Published attempt, outputs and provenance. |
| {download}`study-manifest-1.json <../src/hydrocouple_platform/schemas/platform/study-manifest-1.json>` | Study configuration, lineage and results. |
