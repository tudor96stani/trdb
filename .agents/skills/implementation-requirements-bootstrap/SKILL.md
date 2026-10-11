---
name: implementation-requirements-bootstrap
description: One-off reverse-engineering of candidate correctness requirements from user-selected existing implementation and documentation, with explicit review and approval before activation.
---

# Implementation Requirements Bootstrap

## Purpose and boundaries

Use on demand to propose requirements for **existing implemented functionality**. The user points to the implementation scope and relevant documentation; inspect both. This is a requirements-discovery skill, **not** a test design, test implementation, refactoring, or production-fix skill. Do not change source code, tests, or documentation other than `docs/reqs.md` as explicitly authorized.

Before starting, read `.agents/rules/requirements.md` in full. It is the authoritative contract for format, ID assignment, atomicity, section organization, wording, and lifecycle. Read `docs/reqs.md` (if present), including Active, Pending Review, Superseded, and Rejected entries. Do not duplicate existing requirements or resurrect rejected choices without discussion. No new format or special marker for reverse-engineered requirements is allowed in `docs/reqs.md`.

## Scope selection

Ask the user for the code location(s) and documentation location(s) **only if not already supplied**. Respect the supplied scope; follow directly relevant dependencies when necessary to understand behavior, but report when a necessary source falls outside the selected scope. Do not assume that nearby code, README files, or unrelated modules are in scope. If documentation is absent, say so and proceed from the code and existing requirements, identifying the reduced confidence.

## Source interpretation

Review the selected implementation, relevant tests as evidence where useful, and the selected documentation. Distinguish carefully among:

- behavior explicitly promised by documentation or existing Active requirements;
- behavior clearly and intentionally implemented, supported by structure and tests;
- incidental implementation details that should not be frozen as requirements;
- suspected bugs, contradictions, underspecified behavior, and unclear intent.

Implementation is **evidence**, not authority. Use the established order: user decision > Active requirements > approved test-case specifications > tests > implementation. Documentation is contextual evidence unless the user identifies it as an authoritative specification. Never turn a likely bug into a normative requirement just because the code currently does it. Never infer behavior from a method name alone when its implementation or semantics are unclear.

Look for concrete guarantees around normal operations, boundaries, invalid inputs, errors and failure postconditions, persistence/recovery, consistency, algorithmic paths, and technical invariants relevant to the selected feature. Requirements may be black-box or implementation-specific technical guarantees, but avoid incidental lock-in. Stay focused on correctness; exclude performance/benchmark requirements unless explicitly requested.

## Review workflow

1. **Read and map:** Identify the selected feature(s), summarize relevant observed behavior and existing requirement coverage, and note meaningful documentation/code/requirement conflicts. Give precise file and symbol references in the discussion or review report when available; **do not put source locations or provenance metadata in `docs/reqs.md`**.
2. **Propose, don't declare:** Draft small, atomic, independently testable candidate requirements using the exact existing requirements conventions. Separate well-supported candidates from open questions, suspected defects, and incidental details. Do not silently resolve inconsistencies.
3. **Review interactively:** Present candidates in manageable feature-sized batches. Ask the user **one substantive question at a time** when a decision is needed. Allow the user to accept, reject, amend, or challenge each proposed guarantee. If they request a broader review in one pass, provide it, but still resolve ambiguities one at a time. The user defines intended behavior, not the code. During questioning, answers such as "yes", agreement with a recommendation, or acceptance of a draft or batch clarify intended behavior; they are **not approval to activate requirements**.
4. **Record proposals:** When the user wants candidates recorded, add them only to `## Pending Review` in `docs/reqs.md`, with permanent IDs assigned on entry and temporary Notes for unresolved review issues. Follow existing subsystem/feature grouping and ID conventions. Ask where to place ambiguous cross-cutting requirements. Do not write requirements as unstructured prose or add bootstrap-specific sections.
5. **Final review and approval:** Keep all surviving proposals in `## Pending Review` throughout the questioning and drafting phase, including proposals whose intended behavior is settled. Once the questioning is complete, present the complete final set for the user's official review and explicit approval to activate all of it. Move requirements to `## Active` only after that final approval and drop Pending Review Notes. Do not promote requirements individually or in intermediate batches during the discussion. If the user requests changes during final review, update the pending proposals and present the revised complete set for approval. If final approval is unclear, leave them pending. Follow the standard rejection/supersession rules and retain history; a revised or declined pending draft must not be treated as a superseded Active requirement.
6. **Report gaps:** Summarize unresolved semantics, source conflicts, suspected bugs, and areas not covered by the selected scope separately in chat. Do not manufacture expected behavior or silently modify code/tests to make the requirements appear satisfied.

## Relationship to the other skills

This is a **one-off bootstrap entry point** alongside `requirements-engineer`, not a replacement for its normal forward-looking requirements process. Both produce the **same** `docs/reqs.md` artifact under `.agents/rules/requirements.md`; downstream black-box/white-box test design and test implementation remain unchanged. Do not automatically invoke testing skills or generate test cases.

## Interaction style

Be skeptical of accidental behavior, precise about what is supported by code versus documentation, and concise. Prefer concrete candidate requirements to broad descriptions. Surface one meaningful uncertainty at a time, and wait for the user's decision rather than presenting a large questionnaire.
