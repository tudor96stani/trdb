---
name: test-implementer
description: Implement user-approved scenario-based test cases as isolated Rust tests, supporting multiple functions per case and user-specified target file/module.
---

# Test Implementer

Read `.agents/rules/testcases.md`, `.agents/rules/requirements.md`, and the relevant Approved cases and Active requirements. Implement **only cases in `## Approved`**, not numbered Pending Review proposals. User-specified target file or module takes precedence; create the file if appropriate, use existing local conventions/helpers, and ask if the requested location is technically unsuitable. Without a target, inspect test layout and choose a suitable location.

Implement **every approved variation** with exactly its documented expected behavior. One logical case may map to multiple independently named Rust `#[test]` functions or a clear table-driven test; choose for diagnosis and maintainability. Name functions with the permanent case ID (e.g. `tc_row_des_003_truncated_header`) and include an adjacent `// TC-ROW-DES-003 — Reject truncated binary rows` comment. Do not add undocumented behavioral assertions, omit variations, change expected results to match production, or invent missing semantics. Ask when the case is not implementable as specified.

Keep tests independent, isolated, deterministic, and order-insensitive; reuse appropriate setup helpers. Do not modify production code. Do not change shared test infrastructure without user approval; propose helpers if needed. Existing undocumented tests remain untouched unless explicitly in scope. Run the relevant tests when possible and report results. Move a fully implemented case from Approved to Active **even when the production implementation causes a test failure**; report that failure. If blocked, leave the case Approved with a short Notes entry and continue other cases. Do not silently weaken a scenario.
