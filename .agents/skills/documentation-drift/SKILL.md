---
name: documentation-drift
description: Detect outdated, missing, or contradictory technical documentation by comparing repository docs and decision records with current code, APIs, configuration, and behavior. Report gaps without changing docs.
---

# Documentation drift review

## Inputs and scope
Support `scope=diff` for pre-PR reviews and `scope=repository` for periodic audits. For diff scope, evaluate documents affected by changed behavior (including indirect consumers). For repository scope, sample major modules, documented contracts and configurations; state incomplete coverage.

Read `AGENTS.md`, relevant technical documentation, READMEs, module docs, API contracts, configuration references, and architecture/decision logs. Distinguish **documented intent** from actual implementation. Decision records describe historical reasoning; never silently declare them wrong merely because code evolved.

## Check for
- Documented methods, components, flows, configuration keys, defaults, examples, or interfaces no longer matching reality.
- Undocumented changes to externally observable behavior, commands, setup, schema, or operational requirements.
- Internal architecture explanations or diagrams that no longer reflect dependency relationships or data flow.
- Contradictions between documentation sections.
- Unexplained deviations from accepted decisions; propose a new or amended decision record for the human author rather than rewriting history.

## Quality bar
Verify claims with specific code **and** doc references. Do not invent missing implementation rationale. Do not flag every undocumented private helper; focus on documentation that readers rely on. When absence is the issue, identify the existing doc section where the information belongs, or the appropriate document type.

## Output contract
For each finding use category `documentation-drift`, severity, status, code location, document location (or missing section), the concrete mismatch, reader impact, and recommended documentation change. Do not edit documentation or decision logs. For standalone runs write `.review/documentation-drift-report.md` unless specified otherwise, creating `.review/` if it does not exist; include scope/limitations and explicitly state if no actionable findings exist. Write no other files.
