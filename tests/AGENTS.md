# Testing agent instructions (`tests/AGENTS.md`)

## Purpose and expectations
- This directory contains the project test suite for `dbscoring`.
- All tests must be reproducible, deterministic, and fast enough to support continuous coding.
- Production-grade quality requires both **quick feedback tests** and **full verification tests**.

## Quick tests (for local development)
- Use quick tests before every coding iteration and before commit.
- Quick tests must:
  - run in seconds/minutes,
  - avoid external I/O/network/services,
  - rely on stable fixtures and small synthetic data,
  - cover critical paths around changed code.
- Recommended command:
  - `uv run pytest -m quick`
- Optional stricter quick mode:
  - `uv run pytest -m quick --maxfail=1`
- Keep this set of tests minimal and highly reliable; if a quick test becomes slow, split it:
  - fast deterministic logic into `quick`,
  - heavy cases into `integration` or `full`.

## Full tests (pre-PR / production gate)
- Run before merging or major refactors.
- Full tests must include contract checks, boundary cases, and integration coverage.
- Recommended command:
  - `uv run test` (runs standard pytest suite defined in `pyproject.toml`)
- When release-level confidence is needed, run:
  - `uv run pytest -m "not slow"`
  - `uv run ruff check .`
  - `uv run ty check .`
- Full suite criteria:
  - no flaky behavior,
  - explicit assertions for edge behavior,
  - stable behavior for malformed/missing inputs,
  - deterministic seeds for synthetic data.

## Test organization
- Use a layered structure:
  - `tests/unit/` — pure function/unit behavior (fast).
  - `tests/integration/` — interactions with local files/fixtures/IO.
  - `tests/contract/` — schema and data-contract validation.
- Mark tests with PyTest markers from `pyproject.toml`:
  - `quick`, `unit`, `integration`, `contract`, `slow`, `property`.
- Avoid placing all tests in one flat file; prefer feature-oriented modules.

## Data and fixtures
- Do not read large production datasets directly in tests.
- Prefer `tmp_path` and small explicit fixtures for reproducibility.
- Keep fixtures scoped as tightly as possible and document assumptions in test docstrings.

## Quality rules
- New tests must include:
  - arrange / act / assert structure,
  - clear expected behavior for normal and edge cases,
  - meaningful assertion messages where appropriate.
- A function/feature should have:
  - happy path coverage,
  - at least one edge-case test,
  - one failure-mode test.

## New file conventions
- All test modules: `tests/<scope>/test_*.py`
- Test functions: `test_<behavior_name>()`
- Use parameterized tests for repeated business-rule cases.
