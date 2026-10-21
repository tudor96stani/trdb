# Test-case artifact contract

This file governs `docs/testcases.md`. The document contains specifications only, not agent instructions, execution history, or implementation paths. Use `docs/reqs.md` for project terminology. Read `.agents/rules/requirements.md` when interpreting requirements.

## Structure

Use these top-level sections in order:

1. `# Test Cases`
2. `## Pending Review`
3. `## Approved`
4. `## Active`
5. `## Rejected`

Within each section group by subsystem (`### Row Storage`) and optionally feature (`#### Deserialization`). Avoid unnecessary extra hierarchy. No separate black-box, white-box, ad-hoc, or property-based sections or formats. No glossary, table of contents, status dashboards, execution results, or implementation file paths.

## Pending Review: numbered, ID-free proposals

- Proposed cases have **no permanent test IDs**. Use a Markdown numbered list starting at 1 within each feature grouping; numbering is local to that list, not a global identity. Include a clear title and sufficiently precise scenarios, as shown below.
- If a proposal is removed, rejected, approved, inserted, or reordered, renumber remaining pending proposals consecutively. Numbers are temporary review handles; never use them as stable references in code or elsewhere.
- A rejected **unapproved proposal** is simply removed from Pending Review; no permanent rejection history is necessary. The user alone decides which proposals to approve, revise, or discard.
- Approval assigns a new immutable semantic ID (for example `TC-ROW-DES-001`) and moves the case to `## Approved`. Do not assign IDs merely because proposals have been written or discussed.
- If the user approves only some variations within a proposal, revise/split the proposal as directed before assigning IDs; do not assume approval of unreviewed variations.

Example:

```markdown
## Pending Review
### Row Storage
#### Deserialization

1. **Deserialize a valid mixed row**
   - Given schema `[concrete schema]` and bytes `[exact bytes]`, obtain `[exact row]`.
2. **Reject truncated binary rows**
   - Truncated header → `[defined error category]`.
   - Truncated integer section → `[defined error category]`.
   - Truncated string section → `[defined error category]`.
```

## Approved and Active

- `## Approved` holds user-approved, permanently identified cases awaiting implementation, including blocked cases. `## Active` holds implemented cases even if a test currently fails because production code is defective.
- Use a heading such as `##### TC-ROW-DES-003 — Reject truncated binary rows` under the feature grouping. IDs are globally unique, never reused, and retained across lifecycle transitions. Use a short feature-specific prefix and increasing numeric suffix; gaps are fine. Order by ID within a feature.
- `**Requirements:** ROW-DES-001, ROW-DES-002` is optional; list only approved requirements the scenarios actually verify. Cases informed by implementation may have no requirement ID but must not contradict active requirements.
- `**Notes:**` is optional **only in Approved**, for genuine implementation blockers or handoff questions. Remove notes on promotion to Active. Unresolved expected behavior should be clarified before approval, not disguised as implementation notes.
- A case may contain **multiple related input variations** that verify the same meaningful behavior. Write them as numbered or bulleted scenarios with concrete inputs/construction rules and exact expected outcomes. Use a step/action/expected-result table **only when sequence and intermediate state matter**. Preconditions may be included when necessary; do not force empty boilerplate fields.
- A case must be self-contained enough for an independent implementer to implement **without inventing behavior, test inputs that affect expectations, or assertions**. Reuse approved requirements for common technical contracts; make case-specific values and outcomes explicit. Do not require the implementer to infer intended behavior from production code.
- Examples of grouping: header/integer/string truncations can be variations of **one** truncation case; invalid length/count/offset metadata can be variations of **one** malformed-metadata case if they share the same failure contract. Split only for meaningfully different behavior, risk, state transitions, or useful failure diagnosis.
- **Do not require one Rust test function per documented case.** One approved case may produce multiple independently named Rust test functions (e.g., one per variation) or a well-diagnosed table-driven test. Every implemented variation must remain traceable to the parent case ID and identifiable on failure.
- Use the case ID in test function names (lowercase snake case, e.g. `tc_row_des_003_truncated_header`) and an adjacent comment identifying the case ID and title. Multiple functions may share the same case ID prefix with distinct suffixes. Do not claim the case is fully implemented until every approved variation is covered.
- Test functions must be isolated, independent of execution order and other tests' state. Routine setup/cleanup belongs in test infrastructure; only exceptional setup or isolation constraints belong in a case.
- Do not add undocumented behavioral assertions. Mechanical safety/setup checks are allowed but cannot silently strengthen the specification. Fail fast and keep failures diagnosable.
- Approved and Active cases cannot be materially changed without user approval. Nonsemantic clarification is permitted if reported and code comments remain synchronized.

Example approved case:

```markdown
##### TC-ROW-DES-003 — Reject truncated binary rows

**Requirements:** ROW-DES-002

**Scenarios:**
1. Header is shorter than its required width → deserialization returns the specified truncated-input error.
2. Header is complete but integer payload ends early → the same error.
3. All earlier sections are complete but string bytes end early → the same error.
```

This is a **format illustration**, not an assumption about this project's actual error semantics. Replace placeholders with approved, precise behavior.

## Rejected / retired approved cases

Only previously approved cases need historical retirement tracking. Retain their permanent ID, title, a concise description, retirement date (`YYYY-MM-DD`), and reason in `## Rejected`. Retire only on explicit user authorization. Reinstatement requires explicit user approval; reuse an ID only for the genuinely same scenario, otherwise assign a new ID. Do not automatically retire or rewrite cases when requirements change; report the impact.

## Coverage and scope

- **Optimize for meaningful scenario and failure-mode coverage, not number of test cases or requirement clauses.** Before presenting proposals, consolidate redundant or mechanically similar cases. Avoid a separate case for each field, validation branch, offset, error variant, or boundary unless it exposes distinct behavior or risk.
- A small set of cases can contain many variations. Never omit an important behavior just to meet an arbitrary count. Explain genuinely uncovered risks rather than inflating the list with low-value duplicates.
- Default to deterministic correctness testing. **Do not propose property-based, randomized, or generative testing** unless the user explicitly changes this policy.
- No separate formal/ad-hoc distinction: all cases follow the same scenario-based format. Implementation-informed checks must still be intentional, approved, and precise. Prefer a few representative internal risk paths over exhaustive branch enumeration.
- No blanket obligation to create a case for every requirement clause, or a function for every case. Traceability is many-to-many. Avoid large end-to-end flows unless the user requests them or they are the clearest way to verify a meaningful guarantee.
- When requirements leave expected behavior undefined, ask the user or report a requirements gap. Do not fabricate it or treat observed implementation behavior as normative.

## Authority and implementation restrictions

User decisions > approved Active requirements > approved test cases > automated tests > implementation. Pending requirements and test proposals are not authoritative. Report contradictions instead of silently resolving them.

Only the user approves cases. Test designers propose; test implementers implement only Approved cases, never change production code, and must seek approval before changing shared test infrastructure. A blocked case stays Approved with a short Note while other cases continue. A test that is fully implemented but fails due to a production bug belongs in Active, with the failure reported. Existing undocumented tests are not automatically rewritten or removed.
