# Test-case artifact contract

This file defines the canonical structure, identifiers, content, and lifecycle of `docs/testcases.md`. It is shared by all testing skills. Agent role-specific behavior belongs in `SKILL.md` files, not in `docs/testcases.md`. The test-case document contains test specifications only: no agent instructions, table of contents, summary statistics, glossary, execution results, logs, or implementation paths. Use terminology defined in `docs/reqs.md`.

## Structure and organization

Use these top-level sections, in order:

1. `# Test Cases`
2. `## Active`
3. `## Pending Implementation`
4. `## Rejected`

Under each lifecycle section, group by subsystem (`### B+ Tree`) and feature (`#### Deletion`) as appropriate. Cross-feature groupings are allowed. Each test appears in one primary grouping only; if placement is ambiguous, ask the user to choose. Do not duplicate tests across groupings. Within a feature, order cases by ascending numerical ID. No separate global ad-hoc/white-box section: place those tests alongside other cases for the same feature.

## IDs and titles

- Use short semantic IDs such as `TC-BTREE-INS-001`, `TC-BTREE-DEL-042`, and `TC-WAL-REC-012`; prefixes evolve as needed, without an elaborate taxonomy.
- Assign the ID when a proposed test first enters `Pending Implementation`. IDs are globally unique and immutable across all lifecycle sections; never renumber or reuse an ID.
- Moving a case between lifecycle sections preserves its ID. A materially different scenario receives a new ID; an approved, authorized revision that remains fundamentally the same scenario may keep its ID.
- Titles may be clarified without changing IDs if the scenario's meaning is unchanged; synchronize the exact title in the Rust summary comment when it changes.
- Use the heading `##### TC-BTREE-INS-001 — Insert and retrieve a record` under a `####` feature heading (adapt heading depth if grouping depth differs).

## Lifecycle and approval

- **Pending Implementation** contains both unreviewed proposals and user-approved scenarios waiting to be implemented. Every pending case must have `**Status:** Proposed` or `**Status:** Approved`. The user alone authorizes approval; never infer it from apparent completeness or from an agent's proposal. Test implementers may implement only `Approved` cases.
- Pending cases may have an optional `**Notes:**` field for review questions, exact handoff details, needed helpers or infrastructure, and blockers. Notes are temporary, must not silently override the approved scenario, and are removed when a case moves to Active. A blocked approved case stays pending with a concise blocker note; no separate Blocked status.
- **Active** means approved and implemented, regardless of whether the automated test currently passes. Remove `Status` and temporary `Notes` when moving a case to Active. Active scenarios form a stable regression baseline.
- **Rejected** includes declined or retired scenarios. Preserve ID, title, concise original description, rejection/retirement date (`YYYY-MM-DD`), and reason; full steps are optional. There is no separate Superseded/Obsolete section.
- If an exactly identical rejected scenario becomes relevant again, the user may reactivate the original ID: restore the scenario to Pending Implementation (Proposed or explicitly Approved), then Active after implementation. Retain the prior rejection as a **historical record only**, annotated with the reactivation date; it is not a second live test or second assignment of that ID. If the scenario changes materially, assign a new ID.
- Proposed cases may be edited during review. Approved pending and Active specifications may not be semantically changed without explicit user authorization. Test designers may recommend changes but must not directly modify Active cases. Obvious non-semantic mechanical corrections may be made when appropriate and must be reported.
- Requirement changes do not automatically rewrite approved tests; impact review occurs only on request. Retiring a case moves its historical specification to Rejected on user authorization.

## Detailed deterministic case format

Use this format for ordinary requirements-driven and other scenarios requiring ordered operations:

```markdown
##### TC-BTREE-INS-002 — Reject duplicate key without modifying original

**Status:** Proposed

**Requirements:** BTREE-INS-001, BTREE-INS-002

**Notes:** Optional; remove when implemented.

**Preconditions:**
- An empty indexed table with a unique integer key exists.

| Step | Action | Expected Result |
|---|---|---|
| 1 | Insert `(42, 'original')`. | Insertion succeeds. |
| 2 | Insert `(42, 'replacement')`. | A duplicate-key error is returned. |
| 3 | Query key `42`. | Exactly one row is returned: `(42, 'original')`. |
| 4 | Count all rows. | Exactly one row exists. |
```

Omit `Status` and `Notes` in Active. Omit `Requirements` when no formal requirement applies. `Preconditions` and `Cleanup` are optional when not relevant, but the case must still be self-contained.

- Specify concrete inputs and exact expected results for ordinary deterministic tests. For large datasets or generated inputs, give sufficiently precise construction rules instead.
- Numbered steps execute in the stated order. Do not skip, combine, or reorder them without user approval. Expected results belong at the relevant steps; assert intermediate state when it meaningfully verifies the scenario, not mechanically after every operation.
- Negative tests specify the expected error type/category and important postconditions, including state preservation where required. Do not require exact error-message strings unless their text is explicitly guaranteed.
- Restart, persistence, and crash-recovery scenarios use this same step table; specify the interruption point precisely. Document unavailable deterministic crash/fault-injection capability in pending Notes.
- `**Cleanup:**` is optional, for exceptional cleanup only. Routine cleanup is handled by common test infrastructure and should work even when a test fails where practical.
- No separate Purpose, Objective, Assumptions, Test Level, Implementation, or execution-results fields. Resolve assumptions into preconditions, actions, or expected results; unresolved questions may remain in pending Notes.

## Compact ad-hoc/white-box case format

A small, targeted case may omit formal Preconditions and the step table, but it must still be clear, concise, self-sufficient, and unambiguous:

```markdown
##### TC-BTREE-DEL-042 — Redistribute from left sibling

**Status:** Proposed

Construct an underfull leaf whose left sibling can donate an entry. Perform redistribution and verify that both nodes meet minimum occupancy, entries remain ordered, and the parent separator reflects the new boundary.
```

Add `**Requirements:** ...` if the case genuinely verifies active requirements; otherwise omit it. If a specific key sequence, node capacity, internal layout, or boundary is essential to trigger the path, specify it explicitly; do not leave the implementer to invent test intent or expected results. Implementation-specific assertions (including physical page offsets) are allowed when relevant to this database's actual design. Explain why a magic value matters in the case or in Rust comments. Rust comments may clarify *how* a documented assertion works, never introduce additional behavioral expectations.

## Property-based / generative case format

Property tests use the same ID and lifecycle rules, but replace the step table with generation rules, properties, and reproducibility:

```markdown
##### TC-BTREE-INS-015 — Randomized insertion preserves lookup

**Status:** Proposed

**Requirements:** BTREE-INS-001, BTREE-INS-002

**Preconditions:**
- An empty indexed table exists.

**Input generation:**
- Generate 1–1,000 unique integer keys with associated values.
- Randomize insertion order.

**Properties:**
1. Every inserted key retrieves its associated value.
2. No additional keys are returned.
3. The row count equals the number of unique inserted keys.

**Reproducibility:**
- Preserve/report the random seed and sufficient inputs to reproduce failures.
```

Property tests remain in the suite after finding defects. A meaningful randomized failure should additionally produce a separate deterministic regression scenario, ideally minimized, through the normal proposal/approval process. Failure minimization may be handled by a separate analysis workflow.

## Requirement references and coverage

- `**Requirements:**` lists **all** Active requirement IDs genuinely verified, directly or indirectly, not merely exercised. A case may cover several requirements; several cases may cover the same requirement.
- Only refer to Pending Review requirements when the user explicitly requests designing against them. Such cases remain Proposed until the relevant requirements are approved.
- Do not fabricate expected behavior for missing or ambiguous requirements. Report the gap separately, and formalize expected behavior before adding a requirements-backed case. The user may explicitly approve an ad-hoc case instead of formalizing implementation-specific behavior.
- Requirements-driven black-box design does not inspect production code, but may target precise project-specific behavior defined by requirements. White-box design may inspect the current implementation and propose either requirements or ad-hoc tests.
- No reverse traceability matrix and no test-to-test references or dependencies. Each specification must be independently understandable; repeat necessary data rather than referring to other cases or shared Markdown fixtures.

## Scenario size, independence, and implementation

- Default to small, focused, independently diagnosable scenarios. Large functional/cross-feature flows are a special case **only on user request**. Scenario coherence takes priority over maximizing requirements per test; avoid redundant cases while permitting meaningful overlap. Dependent flows fail fast.
- Prefer separate cases and Rust test functions over parameterized test cases, even for related deterministic variations. Property-based tests are the exception.
- Each test must establish its own state (directly or through approved setup helpers) and must not depend on another test's execution order, side effects, or leftover data. This is a universal rule, not repeated in each specification. Routine setup and cleanup mechanics do not need documenting in every case.
- The default is **one documented case = one Rust test function**. A function may orchestrate subprocesses/restarts. If splitting into several functions would be necessary, ask the user first.
- Each Rust test function name includes the corresponding test ID converted to lowercase snake_case, e.g. `tc_btree_ins_001_insert_and_retrieve_record`. Immediately above the test, include a summary comment with the **exact documented ID and title**, e.g. `// TC-BTREE-INS-001 — Insert and retrieve a record`. If several functions are explicitly approved for one case, each carries the same ID prefix and matching summary comment.
- Implement documented assertions **only**: no extra undocumented behavioral assertions, even if they seem useful. Propose additions for approval and update the specification first. Mechanical setup checks required for safe execution are allowed, but must not introduce extra behavioral expectations.
- Faithfully preserve deterministic step order, inputs, and expectations. Use existing good test helpers; propose new or changed shared helpers, fixtures, fault injection, setup/teardown, or other infrastructure before implementing them. Helpers should abstract mechanics, not hide scenario intent.
- Test level (unit/component/server integration/CLI) is selected during design/implementation review, not permanent test-case metadata. The scenario drives the level. Server behavior is the main focus; full client/server coverage can be smaller.
- A valuable test that is currently unimplementable should **still be proposed with complete steps and expected results** and a pending Notes blocker identifying the missing capability. Do not silently implement a weaker approximation. If blocked, leave it pending and continue unrelated approved cases.
- Once a faithful test is implemented, move it to Active even if production behavior fails; report that failure separately and never modify production code to make it pass. The test implementer may run newly implemented tests, but `docs/testcases.md` never records execution results.
- Existing undocumented Rust tests remain untouched unless the user requests otherwise or the specific task requires changes. An on-demand suite review may report them and propose documentation, but must not automatically rename, rewrite, delete, or migrate them.
