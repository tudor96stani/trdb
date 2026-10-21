---
name: architecture-review
description: Check implementation against the project's declared architecture, module boundaries, dependency direction, invariants, and recorded decisions. Use on feature diffs or for scheduled architecture consistency audits.
---

# Architecture consistency review

## Inputs and scope
Support `scope=diff` and `scope=repository`. In diff scope focus on architectural effects of the feature. In repository scope inspect significant dependency boundaries and recurring consistency problems, recording sampling limits. Prioritize explicit architecture documents and accepted decision records; read `AGENTS.md`.

## Establish intended architecture
Identify documented modules, responsibilities, public contracts, layering, dependency direction, ownership of state, and cross-cutting policies. Where no explicit rule exists, label suggestions as **questions/observations**, not proven violations. Newer accepted decisions may supersede older ones; don't rewrite decision history.

## Check for
- New dependencies crossing explicit boundaries or forming dependency cycles.
- Responsibility leakage between modules/layers.
- Inconsistent application of documented error handling, validation, persistence, or transaction policies.
- Duplicated ownership of state or conflicting sources of truth.
- APIs bypassing established abstractions or violating documented invariants.
- Implementation diverging from still-applicable architecture decisions.

## Quality bar
Show the actual dependency/call/data path and cite the **specific rule or decision** it contradicts. Distinguish unintentional inconsistency from a potentially reasonable design change; suggest human review and possible new ADR when appropriate. Do not enforce personal architecture preferences, add layers by default, or design a new architecture unsolicited.

## Output contract
For each `architecture` finding include severity, status (`introduced`/`pre-existing`/`uncertain`), source and target modules/paths, the breached documented rule with its citation, observed evidence, impact, and a focused recommendation. Keep undocumented design concerns in optional observations. Do not edit code/docs. For standalone runs write `.review/architecture-report.md` unless specified otherwise, creating `.review/` if it does not exist; include scope, limitations and an explicit no-findings outcome. Write no other files.
