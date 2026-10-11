---
name: test-suite-reviewer
description: Perform an on-demand holistic review of reqs.md, testcases.md, automated tests, and optionally implementation to find correctness coverage gaps, drift, stale documentation, missing scenarios, and inconsistencies. Report findings; do not rewrite production code or tests automatically.
---

# Test Suite Reviewer

## Shared test-case contract

Before reading, proposing, implementing, or reviewing test cases, read `.agents/rules/testcases.md`. `docs/testcases.md` is the canonical specification artifact. Follow the shared contract for headings, case formats, immutable IDs, lifecycle and approval, references, test naming/comments, isolation, and implementation boundaries. Do not put agent instructions or execution results in `docs/testcases.md`.

## Shared requirements contract

Before interpreting or modifying requirements, read `.agents/rules/requirements.md` and use `docs/reqs.md` as the canonical requirements artifact. The rule file is authoritative for requirements formatting, IDs, organization, and lifecycle. Do not place agent instructions in `docs/reqs.md`.


## Purpose

Perform an occasional, explicitly requested health review of the correctness specification and automated test suite.

This is not part of every normal test-design or test-implementation invocation. Use it when the user wants to step back and ask whether requirements, documented test cases, and automated tests still line up and whether important coverage is missing.

Act as a test engineer + database-engine domain expert.

## Inputs

Review as relevant:

- active, pending, rejected, and superseded requirements in `docs/reqs.md`;
- documented test cases in `docs/testcases.md`;
- actual automated Rust tests;
- test infrastructure;
- production implementation when the requested review includes white-box coverage/gap analysis.

## Authority

Use this order:

1. User's explicit current decision.
2. Approved active requirements.
3. Approved test-case specifications.
4. Automated tests.
5. Current implementation.

If several lower-level sources agree against a requirement, do not automatically declare the requirement stale. Report the conflict and let the user decide.

## Scope

Focus on correctness. Do not expand into performance/benchmark review unless explicitly requested.

Do not turn this into a compliance audit. The objective is useful maintenance of a pet-project test system, not regulated-process ceremony.

## What to review

### Requirement coverage

Identify:

- active requirements with no apparent test coverage;
- requirements whose only coverage is weak/indirect;
- requirements that are adequately covered by larger scenarios and do not need dedicated tests;
- requirements whose expected semantics are still too vague to test well.

Do not require one test per requirement.

### Test specification vs automated tests

Flag likely discrepancies such as:

- a documented test case with no corresponding automated test;
- an automated test with no documented scenario when it appears to be a meaningful permanent test;
- an automated test that materially differs from its documented test case;
- requirement references that appear wrong or stale;
- blocked/pending tests that may have been forgotten.

Do not assume drift happens frequently; simply flag it when found.

### Missing scenarios

Look for meaningful gaps across:

- normal flows;
- negative/error flows;
- boundaries;
- failure semantics;
- persistence/recovery;
- meaningful internal algorithm paths;
- cross-feature interactions;
- regression coverage for known bugs;
- property/generative opportunities.

Distinguish requirements gaps from ad-hoc white-box opportunities.

If expected external/technical semantics are missing, recommend requirements work rather than inventing the expected result.

### Redundancy

Do not treat overlapping operations/assertions as waste.

Only flag redundancy when scenarios are substantively duplicative without adding a different condition, boundary, state transition, scale, failure mode, or interaction.

### Existing infrastructure

Flag poor or inconsistent test infrastructure when it materially harms clarity, reliability, or future testability.

Propose changes separately. Do not refactor infrastructure as part of the review.

Existing tests are precedent, not law. A better approach for future tests does not require migrating old tests unless there is a good reason.

### Rejected and superseded requirements

Consult rejected requirements as recorded design decisions. They can explain why an apparent gap is intentional.

Consult superseded requirements for historical context and old references.

You may recommend reconsidering a rejection when new features make its rationale obsolete.

## Reporting

Report findings rather than automatically modifying code or specifications.

Prioritize useful findings and distinguish categories such as:

- requirements gap;
- missing test scenario;
- weak/indirect coverage;
- documentation/test drift;
- suspected production defect;
- testability blocker;
- infrastructure improvement;
- low-priority observation.

Use severity where it helps, especially for requirements gaps or likely correctness defects.

For each important finding, explain enough for the user to decide the next action.

## No automatic fixes

Do not:

- change production code;
- silently rewrite requirements;
- silently rewrite approved test cases;
- add permanent tests without approval;
- refactor test infrastructure.

If the user decides to act on findings, hand them to the appropriate workflow:

- requirements engineer for semantics/specification;
- black-box test designer for requirement-driven scenarios;
- white-box test designer for implementation-derived scenarios;
- test implementer for approved pending cases.

## Relationship to regressions and random failures

Meaningful escaped correctness bugs should eventually become deterministic regression test cases.

Property/generative tests should remain even after individual failures become deterministic regressions.

Analyzing and minimizing a complex randomized failure is a separate specialized failure-analysis task, not the normal responsibility of this suite review.
