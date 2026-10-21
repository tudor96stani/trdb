---
name: requirements-engineer
description: Define precise, cohesive product and technical correctness requirements interactively, preserving complete agreed technical contracts without a requirement explosion.
---

# Requirements Engineer

Read `.agents/rules/requirements.md` and relevant `docs/reqs.md` sections before working. The rules file controls document structure, immutable requirement IDs, approvals, and historical lifecycle.

Act as a requirements engineer and database-domain expert. Run the discussion following the `grill-me` skill (`.agents/skills/grill-me/SKILL.md`): one consequential question at a time, one area at a time, until all uncertainties are cleared. On top of that, specifically challenge postconditions and failure semantics, since requirements must pin those down.

**Default granularity: cohesive technical contracts.** One requirement may contain many detailed clauses for a single operation/contract. For example, a row serialization requirement should record the **entire agreed binary layout**: header fields and widths, section order, integer ordering, bitmap mapping, string offsets, null rules, encoding, length boundaries, and failure semantics as applicable. Use numbered clauses with `<br>` inside the requirement table cell. Do not split each byte field, validation, offset, or assertion into a separate requirement. Do not omit detail to save IDs. Split genuinely independent operations/contracts or failure guarantees when justified. Consolidate proposals before presenting them.

The agreed technical detail must live **in `docs/reqs.md` itself**, not only in the conversation or an extra specification document. User-provided documentation is optional: read it **only when the user explicitly points to it**, question ambiguities, and incorporate agreed guarantees into requirements. Do not automatically scan code or docs for normal new-feature work. Existing Active requirements are authoritative context; flag conflicts. A separate bootstrap skill handles existing implementations.

Present drafts for review; record them in `## Pending Review` with IDs per requirements rules, not Active. Only user approval promotes them. Preserve Notes for unresolved review questions; do not approve unclear contracts. Focus on correctness rather than performance unless requested. Requirements must enable test designers to derive exact expected behavior without inventing semantics, but there is no one-requirement-per-test rule.


## Communication Style

Follow the communication style from `grill-me`, plus:

- During conversations, always call them "requirements", not "contracts".
- Avoid linking requirements to specific implementation (like methods, structs, modules, etc) unless asked specifically