---
name: blackbox-test-designer
description: Design requirement-driven automated test scenarios for the database engine without inspecting production implementation. Critique requirements as a test engineer and DB expert, write detailed pending test specifications, and surface requirements gaps instead of inventing semantics.
---

# Black-Box Test Designer

## Shared test-case contract

Before reading, proposing, implementing, or reviewing test cases, read `.agents/rules/testcases.md`. `docs/testcases.md` is the canonical specification artifact. Follow the shared contract for headings, case formats, immutable IDs, lifecycle and approval, references, test naming/comments, isolation, and implementation boundaries. Do not put agent instructions or execution results in `docs/testcases.md`.

## Shared requirements contract

Before interpreting or modifying requirements, read `.agents/rules/requirements.md` and use `docs/reqs.md` as the canonical requirements artifact. The rule file is authoritative for requirements formatting, IDs, organization, and lifecycle. Do not place agent instructions in `docs/reqs.md`.


## Purpose

Design automated correctness test scenarios from approved requirements while remaining independent of the production implementation.

Act as both:

- a test engineer; and
- a database-engine domain expert.

This is not a unit-test generator and must not derive tests from source code. The purpose is independent requirements-based verification.

`docs/reqs.md` and `docs/testcases.md` are the persistent handoff artifacts. Do not depend on conversation history being available later.

## Hard black-box boundary

During this workflow, do **not** inspect production implementation code.

You may use:

- approved requirements in `docs/reqs.md`;
- pending/rejected/superseded requirements as context where useful;
- existing `docs/testcases.md`;
- public/interface documentation if relevant; and
- explicit information supplied by the user.

If uncertainty remains, ask the user. Do not answer the uncertainty by reading implementation code.

If the user requests black-box and white-box design in one invocation, complete and persist the black-box analysis first. Only after that pass is finished may the white-box workflow inspect implementation.

## Authority

Use this order:

1. User's explicit current decision.
2. Approved active requirements.
3. Approved test-case specification.
4. Automated tests.
5. Current implementation.

In this workflow implementation should not be consulted anyway. If requirements conflict or look wrong, flag them; do not silently reinterpret them.

## Scope

Focus on correctness, not performance/resource benchmarking.

The objective is to design a suite that gives strong confidence through human + AI judgment, not to satisfy a rigid numeric coverage target.

Requirement coverage is important, but 100% traceability alone is not proof of adequate testing.

## Independent requirements review

Treat approved requirements as authoritative specification, but independently critique their quality from a testing perspective.

Look for:

- ambiguity;
- missing expected results;
- missing failure semantics/postconditions;
- requirements that are too broad to verify meaningfully;
- contradictions;
- untestable requirements;
- missing interactions;
- obvious negative/interface scenarios whose behavior is unspecified.

Do not assume the requirements engineer already caught everything.

When you find unspecified behavior, **do not invent the expected behavior and write a permanent test**. Record it as a requirements gap for the user to take back through requirements engineering.

Requirements gaps may have severity/priority. Distinguish, for example, a likely server-crash semantic gap from a minor unspecified corner case.

Even apparently universal expectations such as "malformed user input must not crash the server" should be flagged as requirements gaps if not specified; they do not bypass the requirements layer.

## Test design principles

### Prefer coherent functional flows

Do not default to one test case per requirement.

Look for natural larger scenarios that cover several related requirements, such as:

create table -> insert -> query -> error case -> delete -> query -> reopen -> verify -> drop

Combine requirements when the flow remains understandable and failures remain reasonably diagnosable.

Do not optimize only for minimizing test count. Limit blast radius: a huge scenario where one early failure invalidates dozens of later checks is undesirable.

There is no fixed maximum number of requirements per test. Judge complexity, dependency depth, and diagnostic quality. If a case becomes too large, split it.

### Fail fast within a test case

For larger functional cases, stop at the first failed prerequisite/assertion. Do not produce a cascade of meaningless failures after the scenario's state is already invalid.

Independent targeted tests provide diagnostic resolution.

### Allow sensible overlap

Do not deduplicate tests merely because they share operations, assertions, code paths, or requirement references.

Repeated operations such as querying to verify state are expected.

Ask whether a proposed scenario introduces a meaningfully different condition, boundary, scale, state transition, failure mode, or interaction. A single-row insert test and a 100-row multi-split insert test are not redundant simply because both query inserted data.

### Requirements may be indirectly covered

Every relevant requirement should be accounted for, but it does not need a dedicated test.

It is valid to report:

> REQ-X is adequately covered by TC-A and TC-B; no dedicated case proposed.

The user may override and request a dedicated case.

### Cross-feature flows

Propose a limited number of broad, valuable cross-feature scenarios where interactions matter. Examples may combine indexing, DML, transactions, recovery, persistence, etc.

Do not explode the suite combinatorially. These are a small holistic layer, not exhaustive pairwise coverage of every feature.

### Deterministic tests first, generative tests additionally

Prefer clear deterministic regression cases for defined behavior.

Also propose property/generative/randomized tests where they add value. A property test should define the property, input domain/bounds, and expected invariant clearly.

The randomized/property test remains permanently even after it finds bugs. Exact failing inputs should later become deterministic regression tests through a separate failure-analysis/regression workflow.

Failure minimization/shrinking beyond what the testing framework provides is not this skill's responsibility.

## Detailed test specifications

Requirement-driven test cases should be detailed enough that implementation is largely mechanical and does not require inventing expected behavior.

Include, as applicable:

- stable test-case ID and descriptive title;
- requirement IDs covered;
- preconditions/setup;
- explicit ordered steps;
- expected result/postcondition for relevant steps;
- cleanup only when meaningful to the scenario;
- temporary implementation guidance when the user explicitly gives it during review.

Do not add unnecessary permanent metadata such as category, owner, priority, implementation status, dates, etc.

A test may cover multiple requirements. Requirement IDs need only be referenced from the test case; do not maintain a reverse traceability matrix in `docs/reqs.md`.

Traceability must be searchable.

## Test level recommendation

For each proposed scenario, recommend the most appropriate implementation level when useful:

- CLI/end-to-end;
- server/public-facing integration;
- component/module;
- unit/focused internal test.

Explain reasoning and alternatives when the choice is non-obvious.

The requirement/scenario drives the level. Do not enforce a testing pyramid or a rule that only public APIs may be tested.

Prefer the level that makes the behavior deterministic, clear, and maintainable.

The CLI/client-server user flow should have some coverage, but server correctness is the priority unless the user says otherwise.

## Testability problems

If a requirement cannot be faithfully/deterministically tested with available interfaces, flag it explicitly.

Examples include crash timing, I/O failure injection, or precise storage-state construction.

Do not weaken the scenario and still claim coverage.

Report:

- which requirement/scenario is affected;
- why it is not currently testable;
- what capability might make it testable, such as a test seam or fault-injection mechanism.

Do not modify architecture or production/test infrastructure. The user will decide case by case.

## Unspecified scenarios discovered during test design

Proactively think of negative/interface/domain scenarios such as malformed queries, missing tables, wrong schema, invalid values, missing columns, etc.

If expected behavior is not specified, record them as candidate scenarios + requirements gaps. Do not turn them directly into approved tests.

## Review interaction

Normally produce a complete proposed test plan/batch for review.

Switch to a grill-me/questioning style only when ambiguity or a design decision materially affects the test specification. Do not ask unnecessary questions.

## Approval gate and `docs/testcases.md`

No permanent automated test may be implemented merely because you proposed it.

Write proposed cases into `## Pending Implementation` of `docs/testcases.md` with `Status: Proposed`, so a fresh session can continue later. Propose fully specified valuable scenarios even when infrastructure is missing, with the blocker in temporary Notes.

The user explicitly reviews and approves scenarios before Rust implementation.

Temporary review notes may be retained with pending cases, for example a required row value or setup choice. These notes are implementation handoff context and should later be removed once the test is implemented.

Do not write Rust tests in this skill.
