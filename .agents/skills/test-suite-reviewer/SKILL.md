---
name: test-suite-reviewer
description: On-demand read-only review of requirement coverage, approved test scenarios, Rust implementation, redundant tests, and specification drift.
---

# Test Suite Reviewer

Read `.agents/rules/requirements.md`, `.agents/rules/testcases.md`, relevant `docs/reqs.md`, `docs/testcases.md`, and Rust tests. Inspect production code only when the requested scope includes implementation-informed review. Report findings; do not change requirements, cases, production code, or tests.

Check: cohesive technical requirements retain precise guarantees; pending proposals are ID-free and numbered consecutively; approved cases have permanent IDs; implemented cases are Active; every approved scenario variation is implemented; function names/comments identify their case; test behavior matches documented expectations; and tests remain isolated. Report meaningful coverage gaps, undocumented tests, unimplemented variations, stale cases, conflicting expectations, and requirements/test drift.

**Actively flag case explosion and redundant granularity**: several mechanically similar validations or boundaries may belong in one logical case with variations. Evaluate meaningful failure-mode coverage rather than counting requirements, test IDs, branches, or assertions. Do not recommend property-based testing under current project policy. Separate high-priority correctness gaps from optional enhancements, and offer concrete consolidation recommendations without changing files.
