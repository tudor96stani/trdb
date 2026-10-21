---
name: pre-pr-review
description: Run a comprehensive local, read-only review of the current feature branch against its base branch before opening a pull request. Evaluate bugs, security, performance, test gaps, documentation drift, technical debt, and architecture consistency; generate a Markdown report.
---

# Pre-PR review

## Goal and boundaries
Review the work on the current branch before a GitHub PR is opened. **Report only**: do not edit source code, tests, documentation, decisions, configuration, or Git history; do not create PR comments, issues, or Azure DevOps work items. The only permitted write is the requested report under `.review/` (or another user-specified output location). Never commit the report.

Follow repository `AGENTS.md` and existing architecture/decision records as project context. Treat implementation as evidence, not automatically as the intended design. Do not replace the author's decisions with preferences of your own.

## Establish the review scope
1. Determine the base branch from the user's instruction; default to `main`. If appropriate, use `origin/main` when it is available and clearly the intended comparison base. Do **not** fetch or modify refs without permission.
2. Check `git status --short`, current branch, and existence of the base ref. Compute `BASE=$(git merge-base HEAD <base-ref>)` and compare `git diff --find-renames "$BASE" HEAD`. This uses the merge base so unrelated changes on main aren't attributed to this feature.
3. Inspect committed changes and affected modules, callers, tests, docs, public contracts, and configuration. Read enough surrounding context to validate each suspected issue. Inspect renamed/deleted files, not only added lines.
4. By default, **do not** include uncommitted/untracked files in findings. If any exist, prominently note that they were excluded and that the report is not a complete working-tree review. If the user explicitly requests working-tree review, inspect it separately and clearly identify those findings.
5. If the base cannot be resolved or diff cannot be obtained, stop with an actionable error; never silently substitute a full-repository review.

## Required review passes
Read and apply the procedures in these sibling skills (paths relative to repository root):
- `.agents/skills/test-gap-analysis/SKILL.md`
- `.agents/skills/documentation-drift/SKILL.md`
- `.agents/skills/technical-debt/SKILL.md`
- `.agents/skills/architecture-review/SKILL.md`

Pass `scope=diff`, the exact base ref, and the merge-base commit to each review. Apply their criteria yourself if the runtime cannot delegate to separate agents. Do not assume invocation automatically delegates to another skill.

Also perform a correctness/security/performance pass:
- Correctness: violated invariants, incorrect error handling, resource lifecycle, concurrency, transaction boundaries, persistence, backward compatibility.
- Security: trust boundaries, input validation, secrets, authorization, deserialization, injection, dependencies or dangerous defaults (only when implicated by the diff).
- Performance: credible algorithmic, allocation, I/O, locking, query, or scaling regressions. Require a plausible workload; label unmeasured performance risks as hypotheses.

Use available tests, linters, static analyzers, and build commands **only when safe and permitted**. Never claim they ran unless they did. Read-only review does not authorize commands that modify tracked files, install dependencies, access secrets, or contact external services.

## Triage
- Only include findings with concrete, inspectable evidence. Verify each against context and existing tests before reporting.
- Separate **introduced by this branch** from **pre-existing / uncertain** findings. Do not accuse the feature of a previously present issue.
- Use severities: `critical` (urgent compromise/data loss), `high` (likely significant defect), `medium` (meaningful risk or maintainability cost), `low` (minor but actionable). Never inflate to fill categories.
- Avoid duplicate findings across passes: use one primary category and optional secondary categories.
- Every finding needs a unique local ID (`PR-001`, etc.), category, severity, status (`introduced`, `pre-existing`, `uncertain`), `path:line` (or explanation if no exact line), evidence, impact, and an actionable recommendation. Describe a way to verify when feasible.
- Exclude style-only preferences and speculative suggestions. An empty findings list is valid.

## Output
Write `.review/pre-pr-report.md` unless the user specifies another location. Ensure `.review/` is ignored or warn if it isn't; don't change `.gitignore` without permission. Use this format:

```markdown
# Pre-PR Review
- Head: `<branch>` (`<head-sha>`)
- Base: `<base-ref>` (merge base `<sha>`)
- Scope: committed diff only | explicit working-tree scope
- Checks run: <commands and results, or 'Not run'>
- Limitations: <missing context/uncommitted changes/etc.>

## Summary
<brief assessment and counts by severity; highlight blocking findings>

## Findings
### PR-001 — <short title>
- Category: correctness | security | performance | test-gap | documentation-drift | technical-debt | architecture
- Severity: critical | high | medium | low
- Status: introduced | pre-existing | uncertain
- Location: `path:line`
- Evidence: <specific behavior, document, or code relationship>
- Impact: <why it matters>
- Recommendation: <what to investigate/change without modifying code>
- Verification: <test or manual check, if useful>

## Non-blocking observations
<Optional, short, clearly differentiated from findings>

## Review coverage and limitations
<Areas inspected, checks not run, assumptions>
```

If no findings exist, explicitly write `No actionable findings identified` and still include coverage and limitations. Keep the report readable and prioritized, not a dump of every observation.
