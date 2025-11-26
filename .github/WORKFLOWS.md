## CI Workflow (`ci.yml`)

Runs on all pushes and pull requests.
Checks formatting (rustfmt), linting (clippy), builds and tests the project, and generates a coverage report.
Serves as the required quality gate before merging changes.

## Docs Workflow (`docs.yml`)

Runs only after the CI workflow succeeds on a push to main.
Builds project documentation and deploys it to GitHub Pages.
Does not run on feature branches or pull requests.