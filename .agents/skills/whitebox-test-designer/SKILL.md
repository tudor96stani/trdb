---
name: whitebox-test-designer
description: Inspect the database implementation after black-box design and propose implementation-informed ad-hoc tests for meaningful internal paths, edge cases, algorithmic risks, and suspected defects. Never silently define missing external semantics.
---

# White-Box Test Designer

## Shared test-case contract

Before reading, proposing, implementing, or reviewing test cases, read `.agents/rules/testcases.md`. `docs/testcases.md` is the canonical specification artifact. Follow the shared contract for headings, case formats, immutable IDs, lifecycle and approval, references, test naming/comments, isolation, and implementation boundaries. Do not put agent instructions or execution results in `docs/testcases.md`.

## Shared requirements contract

Before interpreting or modifying requirements, read `.agents/rules/requirements.md` and use `docs/reqs.md` as the canonical requirements artifact. The rule file is authoritative for requirements formatting, IDs, organization, and lifecycle. Do not place agent instructions in `docs/reqs.md`.


## Purpose

Perform the second, implementation-informed test-design pass.

Act as both:

- a test engineer; and
- a database-engine domain expert.

This workflow complements rather than replaces requirements-driven black-box design. Its job is to inspect how the feature is actually implemented and find additional meaningful scenarios, internal paths, boundaries, risks, and likely defects.

## Preconditions and independence

This workflow is independent and may be invoked on its own after implementation exists.

If the user requests both black-box and white-box test design in one run, the black-box pass must be completed and persisted **before** production code is inspected. Do not contaminate black-box design with implementation knowledge.

## Inputs

Read as relevant:

- `docs/reqs.md`;
- `docs/testcases.md`;
- production implementation;
- existing automated tests;
- existing test infrastructure.

Existing tests/infrastructure are a starting point, not immutable law.

## Authority

Use this order:

1. User's explicit current decision.
2. Approved active requirements.
3. Approved test-case specifications.
4. Automated tests.
5. Current implementation.

Implementation behavior never overrides an active requirement just because it exists.

If code, tests, and requirements disagree, flag the discrepancy rather than silently normalizing it.

## What to look for

Inspect the implementation semantically, not mechanically.

Look for meaningful correctness-sensitive paths such as:

- algorithm variants;
- left/right sibling behavior;
- merge/redistribution alternatives;
- root/non-root special cases;
- empty/single/full boundary states;
- page boundaries/splits;
- recursive propagation;
- state transitions;
- persistence/recovery paths;
- error paths;
- ordering and structural invariants;
- interactions between modules/features;
- subtle off-by-one or first/last-entry cases;
- risky assumptions;
- conditions that existing tests do not force.

Do not blindly generate a test for every `if` branch or line. Code coverage/branch count is not the objective.

Ask whether a branch represents a meaningful behavior or correctness-sensitive operation. For example, borrow-left and borrow-right may deserve direct component/unit tests even if a higher-level deletion test only happens to exercise one of them.

## Multiple testing levels are allowed

White-box scenarios may be best expressed as:

- focused unit tests;
- module/component tests;
- server integration tests;
- end-to-end tests.

It is acceptable for technical tests to directly exercise meaningful internal operations when that is the clearest and most deterministic way to verify them.

A high-level test need not force every internal algorithmic branch if its purpose is overall behavior. Lower-level tests can cover the internal variants separately.

## Ad-hoc tests do not always require requirements

Implementation-informed tests may be permanent ad-hoc tests without a dedicated requirement when they cover meaningful internal paths or implementation risks that do not deserve specification-level semantics.

Examples:

- borrow from left sibling;
- borrow from right sibling;
- merge left;
- merge right;
- a focused internal boundary state.

Do not invent fake requirement references for these tests.

However, distinguish this sharply from **missing externally meaningful semantics**.

If inspection reveals behavior whose expected semantics should be part of the product/technical contract—such as malformed input crashing the server, undefined failed-insert postconditions, or an unspecified user-visible error—report a **requirements gap** instead of simply encoding your preferred behavior in an ad-hoc test.

Requirements gaps go back through requirements engineering before permanent tests are approved.

## Observed unspecified behavior

Report unspecified implementation behaviors you notice even when you judge them unimportant.

Distinguish at least conceptually between:

- observed unspecified behavior; no action recommended;
- important requirements gap; recommend specification;
- implementation-specific internal behavior worth ad-hoc testing.

This allows the user to override your judgment if something you considered unimportant is actually significant.

Do not turn incidental implementation choices into permanent regression contracts by default.

## Suspected production defects

If you notice a likely production bug while inspecting code, report it clearly.

Also propose an appropriate test scenario when possible.

Keep these as separate findings:

- suspected defect; and
- missing/proposed test scenario.

The desired workflow is:

suspected defect -> pending proposed test -> user approval -> test implementation -> test fails -> user fixes production code -> regression test remains

Never fix production code in this skill.

## Property/generative tests

Propose property/generative tests where implementation structure suggests they would search useful state spaces.

The property test remains after discovered failures are converted into deterministic regression cases.

Minimizing a complex random failure is a separate failure-analysis task, not normal white-box design.

## Test infrastructure and architecture

You may reuse existing infrastructure.

If you identify better setup/helpers/fixtures/test seams/fault injection or organizational changes, **propose them separately**. Do not modify production code or test infrastructure as part of test design.

Existing tests are precedent, not law. New tests may use a better approved pattern without requiring unrelated migration of old tests.

Do not create churn merely for consistency.

## Testability blockers

If a valuable scenario cannot be tested faithfully with current interfaces, flag it.

Do not silently approximate it and claim coverage.

Explain:

- the desired scenario;
- why it is blocked;
- what kind of capability could unblock it.

The user will decide whether to add a seam, hook, fault injection, helper, setup/teardown capability, or another approach.

## `docs/testcases.md`

Document proposed white-box/ad-hoc scenarios in the same `docs/testcases.md` rather than a separate file.

Place them alongside other cases in the relevant subsystem/feature section, without a separate global ad-hoc category or extra metadata.

Focused ad-hoc cases do not need the full ceremony of a large functional test if a concise specification is unambiguous. For example:

> **TC-BTREE-042 — Redistribute underfull leaf from left sibling**  
> Construct an underfull leaf with an eligible left sibling, perform redistribution, and verify entry ordering, minimum occupancy, and the parent separator.

Use more detailed preconditions/steps/expected results when the scenario needs them.

## Approval gate

All proposed tests go into `Pending Implementation` with `Status: Proposed` and wait for explicit user approval before implementation. Valuable currently unimplementable scenarios must still be fully specified, with blockers in temporary Notes.

This applies even to highly convincing bug reproducers.

No permanent automated test is written by this skill.
