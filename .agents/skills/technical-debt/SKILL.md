---
name: technical-debt
description: Identify substantiated technical debt, complexity hotspots, duplicated logic, and maintenance risks in changed code or across a repository. Prioritize evidence-based opportunities; never refactor automatically.
---

# Technical debt review

## Inputs and scope
Support `scope=diff` and `scope=repository`. In diff scope identify debt introduced or materially worsened by the branch; distinguish inherited issues. In repository scope prioritize repeated patterns and maintenance hotspots using available churn, complexity, tests, and dependency context; do not claim quantitative metrics unless measured.

Read `AGENTS.md`, project conventions, and accepted decisions before calling an approach debt. Intentional tradeoffs and learning-oriented implementations are not automatically defects.

## Check for
- Duplicated business logic that is genuinely likely to diverge (not simply similar code).
- Hard-to-change coupling, oversized responsibilities, unnecessary indirection or abstraction layers.
- Poorly isolated side effects or difficult-to-test dependencies where they obstruct maintenance.
- Dead paths or obsolete compatibility work, only with credible usage evidence.
- Repeated error-prone patterns, fragile invariants, resource management complexity.
- Unaddressed TODOs only when backed by a concrete consequence; not a list of all TODOs.
- `panic!` or methods that can result in an unintended panic

## Quality bar
Explain **actual maintenance cost** and where it appears. Prefer a few meaningful issues to style complaints. Never recommend a refactor merely because another design is fashionable. Flag risky changes with migration/testing implications. Record performance concerns only with defensible reasoning and without inventing benchmark results.

## Output contract
Each `technical-debt` finding needs severity, `introduced`/`pre-existing`/`uncertain`, locations, evidence (including repetition or dependency impact where relevant), cost, and the smallest plausible remediation with verification implications. Do not change code. For standalone invocation create `.review/technical-debt-report.md` or requested path; state reviewed scope, limitations, and if no actionable findings exist. Write no other files.
