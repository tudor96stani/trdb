# Requirements artifact contract

This file defines the canonical structure, wording, identifiers, and lifecycle of `docs/reqs.md`. It does not define agent roles or workflows; those belong in individual skills. `docs/reqs.md` contains project requirements only, never agent instructions or formatting rules.

## Document structure

Use exactly these top-level sections, in this order:

1. `# Requirements`
2. `## Terminology` (omit if no terms need defining)
3. `## Active`
4. `## Pending Review`
5. `## Superseded`
6. `## Rejected`

Under each lifecycle section, organize requirements by subsystem (`### B+ Tree`) and, where useful, feature (`#### Deletion`). Each lowest-level requirement grouping has **one Markdown table**; do not interleave prose or free-standing requirements. Use the same subsystem/feature organization in the historical sections where practical. A requirement appears in exactly one lifecycle section and one subsystem/feature grouping. If its appropriate grouping is ambiguous, ask the user to choose; do not decide unilaterally. Do not duplicate cross-cutting requirements in several sections: place them once and reference their ID when needed.

No contextual or introductory prose belongs in `docs/reqs.md`. Headings are organizational only, not normative. Do not hide requirements in notes, headings, or definitions.

### Tables

Active — exactly two columns:

```markdown
| ID | Requirement |
|---|---|
| BTREE-INS-001 | Inserting a record with a unique key shall make that record retrievable by its key. |
```

Pending Review — exactly three columns; Notes may be empty:

```markdown
| ID | Requirement | Notes |
|---|---|---|
| BTREE-DEL-014 | When a non-root leaf underflows and an eligible left sibling can donate an entry, the tree shall redistribute an entry from that sibling. | Confirm separator-update behavior. |
```

Superseded — exactly five columns:

```markdown
| ID | Requirement | Date | Replaced By | Reason |
|---|---|---|---|---|
| BTREE-DEL-004 | Deletion shall preserve B+ tree structural validity. | 2026-10-11 | BTREE-DEL-014, BTREE-DEL-015 | Replaced with independently testable guarantees. |
```

Rejected — exactly four columns:

```markdown
| ID | Requirement | Date | Reason |
|---|---|---|---|
| BTREE-DEL-020 | Redistribution shall always prefer the left sibling. | 2026-10-11 | Sibling selection is intentionally not guaranteed. |
```

Use ISO `YYYY-MM-DD` dates for Rejected and Superseded only. Preserve original requirement text and ID in historical rows. If a replacement does not exist, leave `Replaced By` empty rather than inventing one. Escape literal table pipes as `\|` and keep each requirement in a single Markdown table cell.

Sort requirements **by ID ascending** within each table (numeric suffix order, not arbitrary rearrangement). Do not reorder by topic or priority.

## Requirement wording

- Each row states one independently meaningful, independently testable guarantee. Separate distinct behaviors such as borrow-left, borrow-right, merge, separator update, recursive underflow, and root collapse when they constitute separate guarantees.
- Requirements may specify externally observable behavior **or** technical/internal/algorithmic behavior. Both levels are legitimate; some overlap is acceptable.
- Specify triggering conditions, expected behavior, and meaningful invariants/postconditions precisely enough to design tests without inventing semantics.
- Include important failure/error semantics and atomicity/consistency guarantees where relevant. Do not leave essential behavior to test designers to guess.
- Prefer clear normative wording, commonly “shall”; avoid unnecessary ceremony or vague assertions such as “handle errors correctly” or “preserve all invariants.”
- Do not bundle unrelated guarantees merely because they belong to the same operation.
- Do not add routine metadata: no priority, implementation status, test status, author, creation date, verification method, or per-row category.
- Avoid requirement-relationship metadata (parent, depends on, related); use an explicit ID reference in requirement text only when necessary.

## Terminology

If needed, maintain a **small** `## Terminology` section containing one table:

```markdown
| Term | Definition |
|---|---|
| Minimum occupancy | Project-specific definition, when established. |
```

Only define a term if its **project-specific precise meaning** matters in multiple requirements. Do not create a general database glossary. Definitions clarify terminology; they must not introduce hidden behavioral guarantees.

## Identifiers and changes

- Assign a permanent ID **when a proposal first enters Pending Review**. Before then, discussion drafts need not have IDs.
- IDs are globally unique across Active, Pending Review, Superseded, and Rejected; never reuse or renumber them.
- Use short semantic prefixes plus numeric suffixes, e.g. `BTREE-INS-001`, `BTREE-DEL-001`, `WAL-REC-001`, `TX-COMMIT-001`. Create prefixes as needed rather than defining an exhaustive taxonomy. Avoid excessively long prefixes.
- An ID stays unchanged as its requirement moves between lifecycle sections, including rejection or supersession.
- Non-semantic wording clarification retains the ID. A materially different guarantee gets a **new ID** (assigned on entry to Pending Review); the old requirement is superseded with its original ID once the change is approved. Never silently repurpose an existing ID.
- Numeric gaps are allowed. IDs need not be consecutive.

## Lifecycle and authority

- **Active:** explicitly user-approved, authoritative requirements; only `ID | Requirement`. Approval moves the row from Pending Review to Active and **drops the Notes column**. Resolve notes that affect expected semantics before approval.
- **Pending Review:** proposals awaiting explicit user approval; `ID | Requirement | Notes`. Notes are temporary review context, questions, or decisions to confirm. Pending requirements are **not authoritative**. Agents may consult them for context but must not silently treat them as approved requirements.
- **Rejected:** intentionally declined proposals, preserving ID, text, rejection date, and reason. Consult this history when related decisions arise; reconsider only with user involvement.
- **Superseded:** previously valid requirements replaced or refined, preserving ID, original text, date, replacement ID(s) when applicable, and reason. These are historical, not normative.
- Only the user authorizes promotion, rejection, supersession, or semantic changes. Flag contradictions rather than resolving them silently.

The authority order is: **user → Active requirements → approved test-case specification → automated tests → implementation**. Pending, rejected, and superseded requirements provide context but do not override Active requirements.
