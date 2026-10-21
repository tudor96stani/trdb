---
name: test-gap-analysis
description: Review a Git diff or repository for meaningful missing test scenarios, weak assertions, and regression risks without writing tests. Use during pre-PR reviews or scheduled test audits.
---

# Test gap analysis

## Inputs and scope
Support `scope=diff` (default when invoked from pre-pr-review) and `scope=repository` (scheduled/baseline audit). In diff scope, use the caller's merge base and examine changed behavior plus surrounding code/tests; don't treat unrelated legacy coverage gaps as newly introduced. In repository scope, prioritize high-risk modules and representative paths; disclose coverage limits instead of claiming exhaustive analysis.

Read `AGENTS.md`, test conventions, relevant implementation, fixtures, and existing tests. Identify behaviors and invariants before suggesting cases. Prefer test commands/coverage reports already available; do not infer actual coverage percentages without measurements.

## Check for
- Missing boundary, error, cancellation, timeout, and invalid-input scenarios.
- Persistence, concurrency, transaction, retry, and recovery cases when the code has those concerns.
- Regressions in public behavior or previously supported cases.
- Weak assertions, tautological tests, tests that merely restate mocks, or inadequate failure expectations.
- Missing integration-level tests where unit tests cannot validate the behavior.
- Untested behavior introduced by changes to configuration, serialization, schemas, or APIs.

## Quality bar
Recommend a test only when you can state **the behavior to test, a realistic failure mode, and the expected outcome**. Avoid trivial getters/setters, raw line-coverage targets, implementation-coupled tests, or duplicates of existing tests. Do not write tests or alter source. Note when an issue is already covered elsewhere.

## Output contract
Report findings using the requesting workflow's format. For each finding include severity; `introduced`, `pre-existing`, or `uncertain`; exact `path:line` of the behavior (plus existing test references if relevant); missing scenario; risk; and a concise recommended test setup/assertion. Classify as `test-gap`. For standalone runs, produce `.review/test-gap-report.md` unless another path is specified, creating `.review/` if it does not exist; include scope, evidence, limitations, and `No actionable findings identified` when applicable. Write no other files.
