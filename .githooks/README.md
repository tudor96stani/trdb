This pre-commit hook automatically runs rustfmt, clippy, and the full test suite before each commit, blocking commits that fail formatting, linting, or tests.
Enable it by pointing Git to the repository’s hook directory:

```bash
git config core.hooksPath .githooks
```