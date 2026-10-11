---
name: test-implementer
description: Implement explicitly approved pending test cases as automated Rust tests. Keep implementation mechanical, run new tests, report failures/blockers, and never modify production code or unapproved test infrastructure.
---

# Test Implementer

## Shared test-case contract

Before reading, proposing, implementing, or reviewing test cases, read `.agents/rules/testcases.md`. `docs/testcases.md` is the canonical specification artifact. Follow the shared contract for headings, case formats, immutable IDs, lifecycle and approval, references, test naming/comments, isolation, and implementation boundaries. Do not put agent instructions or execution results in `docs/testcases.md`.

## Shared requirements contract

Before interpreting or modifying requirements, read `.agents/rules/requirements.md` and use `docs/reqs.md` as the canonical requirements artifact. The rule file is authoritative for requirements formatting, IDs, organization, and lifecycle. Do not place agent instructions in `docs/reqs.md`.


## Purpose

Implement approved pending test specifications from `docs/testcases.md` as automated Rust tests.

This skill is intentionally less creative than the test-design skills. The approved scenario is the contract.

The user may invoke this in a completely fresh session, so all necessary handoff information must come from repository artifacts, especially `docs/testcases.md`.

## Authority

Use this order:

1. User's explicit current decision.
2. Approved active requirements.
3. Approved test-case specification.
4. Automated tests.
5. Current implementation.

For this workflow, the approved test-case specification is the immediate implementation contract. Do not reinterpret it based on what production code currently does.

Do not automatically re-audit requirement changes before implementing pending tests unless the user explicitly requests such a review.

## Hard boundaries

### Never modify production code

You may run tests and discover production defects, but you must not change production code to make tests pass.

A failing approved test is a valid result.

### Do not change test infrastructure without approval

Do not silently:

- add or redesign test fixtures;
- add new generic helpers;
- reorganize tests;
- change setup/teardown architecture;
- add fault-injection hooks;
- change visibility solely for testing;
- refactor existing test infrastructure;
- modify production architecture for testability.

If such a change appears useful or necessary, flag/propose it separately and continue with other implementable tests.

Reuse existing test infrastructure where appropriate.

Existing tests are a starting point, not law. If a better pattern would help new tests, propose it. Once approved, new tests may use it without forcing unrelated existing tests to migrate.

## Test-code style

Optimize for clarity and correspondence with `docs/testcases.md`, not maximal DRYness.

Helpers should abstract mechanics, not hide test intent.

Good examples:

- `create_table(...)`
- `insert_row(...)`
- `query(...)`
- `restart_database(...)`

Avoid over-abstracted helpers that hide the actual scenario, such as a helper that silently performs the entire state sequence the test is supposed to demonstrate.

A reader should be able to see the important scenario steps directly in the test body.

Prefer one Rust test function per documented case and separate deterministic tests over parameterization. Use helpers for setup mechanics, not to conceal intent; propose any new shared helper or infrastructure for approval.

## Choosing implementation level

Follow the approved/reviewed recommendation when one exists.

Tests may legitimately be:

- CLI/end-to-end;
- server/public integration;
- component/module;
- focused unit tests.

The objective is faithful automated coverage, not adherence to a rule that only public APIs may be tested.

Server correctness is the primary focus; CLI/client-server communication may have a smaller end-to-end layer unless otherwise specified.

## Implementing pending cases

For each approved pending case:

1. Read the full scenario and any temporary implementation notes.
2. Implement it faithfully.
3. Keep fail-fast semantics for dependent functional flows.
4. Include the test-case ID in the Rust function name (lowercase snake_case) and put a summary comment immediately above the test containing the exact documented ID and title. Keep any documented requirement references searchable without inventing new ones. Implement only documented behavioral assertions, preserving step order and concrete test data.
5. For ad-hoc white-box tests without requirement references, do not invent requirement IDs.
6. Run/compile the new test as appropriate.
7. Record the outcome.

## When the specification cannot be implemented faithfully

Do not "do the best approximation" and claim the test is implemented.

If a test cannot be implemented as specified:

- skip that test;
- continue implementing unrelated approved tests;
- leave the case pending;
- add/retain a concise temporary blocker note in `docs/testcases.md` when useful;
- report the issue clearly, e.g. `TC-BTREE-023 cannot be implemented as specified; please advise`;
- explain why and, if useful, propose options.

The user will decide how to unblock it.

## Corrections discovered during implementation

Distinguish semantic changes from obvious mechanical corrections.

### Semantic change

If the scenario, expected behavior, assertions, step order, or meaningful setup must change, do not silently alter it. Skip that case and ask for approval. Do not add extra undocumented assertions.

### Mechanical correction

For an obvious typo or trivial mistake that does not change the intended scenario/semantics, you may correct it, implement the test, and clearly report that you made the correction.

Use judgment conservatively.

## Running tests

You may and should run the tests you implement to validate the test code.

If an approved test fails against production behavior:

- do not change production code;
- confirm as far as practical that the test faithfully implements the approved scenario;
- report expected vs observed behavior;
- leave the automated test in place.

The AI is not responsible for continuously operating/monitoring the full suite. Running affected/new tests during implementation is expected.

## Completion semantics

Implementation completion is independent of whether production passes the test.

For a batch of pending cases:

- implemented + passing -> implementation complete;
- implemented + failing against production -> implementation complete, failure reported;
- blocked/unimplementable -> remains pending;
- requires semantic clarification -> remains pending;
- unrelated cases should continue even when one case is blocked.

A test that is correctly implemented but currently failing should leave the pending implementation area and become a normal permanent documented test case.

Blocked/unresolved cases remain pending with concise temporary notes.

## Updating `docs/testcases.md`

`docs/testcases.md` is the human-readable specification of the suite.

After successful implementation of a case:

- move it out of the pending implementation area into the normal permanent section;
- remove temporary implementation-only notes that have served their purpose;
- retain the permanent scenario specification;
- do not add unnecessary lifecycle metadata.

If a case remains blocked, keep it pending.

## Bugs and regressions

Do not add unapproved regression tests opportunistically.

When a known bug already has an approved pending regression scenario, implement it normally. It may fail until the user fixes production code.

If you discover a new bug while implementing another test, report it and suggest a new pending scenario rather than adding an unreviewed permanent test.
