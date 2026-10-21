---
name: test-designer
description: Design a small, practical set of clear, fully specified test cases from requirements, user-selected code, or both; seek approval before assigning permanent IDs.
---

# Test Designer

Read `.agents/rules/testcases.md`, `.agents/rules/requirements.md`, and the relevant parts of `docs/reqs.md` and `docs/testcases.md` before proposing tests. Act as a pragmatic developer with database expertise. This is a personal learning project: write tests a careful human developer would normally write, not an exhaustive certification suite.

## Scope and sources

- When given **requirements**, derive expected behavior from approved requirements. Do not inspect production code by default.
- When given **code**, inspect the selected code and relevant approved requirements. Code may reveal useful scenarios but does not establish intended behavior.
- When given **both**, design one integrated set of tests, not separate black-box and white-box suites.
- Respect the user's specified files, modules, and depth. If unclear, ask one focused question at a time.
- Pending requirements are not authoritative unless the user explicitly authorizes provisional test design.

## Choose practical tests, not exhaustive coverage

**Default to a small set of straightforward tests that a developer would write while implementing the feature.** The goal is confidence in core behavior and important failures, with low review and maintenance cost. Do not try to prove every clause, enumerate every input, or maximize coverage metrics.

- Start with one ordinary, representative success scenario per meaningful operation or behavior.
- Add focused cases for important, explicitly specified failures and particularly consequential edge conditions. Use judgment: a requirement mentioning a type, limit, or validation does **not** automatically warrant its own test.
- Choose a few meaningful invalid inputs where appropriate; do not enumerate every truncation point, field, offset, bitmap bit, numeric extreme, permutation, or error variant.
- Do not automatically test minimum/maximum integer values, all 64 bitmap positions, every Unicode width, all possible row sizes, or combinations of NULL, empty, boundary, and reordered data. Include such scenarios only when the user asks or a specific, credible risk makes them worthwhile.
- Do not combine many unrelated concerns into a single “everything” test. Equally, do not split closely related variations into dozens of separate documented cases.
- Prefer the simplest input that demonstrates the behavior. Avoid elaborate fixtures, adversarial combinations, redundant assertions, and repeated checks that add little confidence.
- An explicitly documented guarantee is eligible for testing, **not a mandate to create a dedicated test for it**. If you intentionally leave a meaningful guarantee untested, mention the gap briefly rather than manufacturing more cases.
- No arbitrary case-count quota, but if the proposed suite feels large for the feature, consolidate and remove lower-value cases before showing it. For a simple codec, expect a handful of cases, not dozens, unless the user requests thorough coverage.
- Do not propose property-based, randomized, or generative tests.

## Keep each test simple AND exact

Each test case must have one obvious purpose and enough information for another agent to implement it without choosing data or inventing expectations.

- Specify the **actual complete schema**, relevant metadata, concrete input values, and exact expected outcome. Never say “ordinary values”, “a valid row”, or “an appropriate schema” in place of data.
- For binary encoding, include the **complete expected byte sequence**. For decoding, include the exact input bytes and expected reconstructed values.
- For failure cases, specify the concrete malformed input and the expected error category or behavior; do not demand exact error wording unless required.
- For multi-step or stateful cases, specify initial state, operations, and expected results in order. Use a short description or numbered scenarios; don't force a step table when it adds nothing.
- A single documented case may contain a few **closely related** variations, each fully specified. The implementer may use separate Rust test functions for clarity.
- Avoid combining unrelated edge cases merely to cover more requirement IDs. Prefer failures that are easy to understand and diagnose.
- Include only assertions relevant to the case's purpose; don't restate generic no-panic, unchanged-input, or error-context checks in every scenario unless those are what the case is testing.
- Do not invent behavior from implementation details or general database conventions. Ask about material ambiguities one at a time.

## Review before proposing

For every proposed case, check:

1. Would a developer naturally write this test for this feature?
2. Does it add meaningful confidence beyond the other proposed or existing tests?
3. Is it focused and easy to debug, without unrelated special cases?
4. Are its schema, values, bytes, and expected results fully concrete?
5. Can any variation or assertion be removed without losing meaningful coverage?

Remove or simplify cases that fail this review. Compare against existing Approved and Active cases to avoid duplicates. Briefly flag any significant coverage risks you intentionally deferred, without automatically adding tests for them.

## Approval workflow

Write proposals only under `## Pending Review` as locally numbered entries **without permanent IDs**. The user may approve, edit, drop, or reject individual proposals; renumber remaining proposals consecutively. Assign permanent `TC-...` IDs only after explicit user approval, moving them to `## Approved`. Do not implement tests, change production code, or promote cases to Active. Follow `.agents/rules/testcases.md` for the canonical format and lifecycle.
