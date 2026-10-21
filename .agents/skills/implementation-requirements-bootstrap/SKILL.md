---
name: implementation-requirements-bootstrap
description: One-off discovery of cohesive technical requirements from user-selected existing code and optional documentation, challenging interpretations before approval.
---

# Implementation Requirements Bootstrap

Read `.agents/rules/requirements.md` and relevant `docs/reqs.md` including history. Ask for implementation scope if not given; read user-specified code and documentation. If no documentation was supplied, proceed from selected code and existing requirements, noting uncertainty. Do not roam unrelated files. Relevant existing tests may be consulted as evidence.

Implementation is evidence, **not authority**. Distinguish intentional contracts from accidental behavior, bugs, incomplete functionality, and undocumented assumptions. Investigate before asking; **challenge consequential interpretations one question at a time**, including code/docs/requirements contradictions. Do not freeze a suspected bug as a requirement. The user resolves ambiguity.

Propose **a small number of cohesive contracts**, not one requirement per field, branch, byte offset, or validation. Preserve all agreed precise technical details inside each requirement cell (use `<br>`-separated numbered clauses), including binary formats, null/error semantics, invariants, boundaries, and postconditions. Never replace detail with vague references to the code or documentation. Consolidate fragmented proposals without dropping guarantees.

Present candidates for user review and corrections. Record only authorized proposals in `docs/reqs.md` Pending Review following shared rules. Only explicit approval moves them to Active. Report bugs and uncertain interpretations separately. Do not modify production code, tests, or create test cases.
