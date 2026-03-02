# PD Pull Request Template

> **Template source**: Read `.github/pull_request_template.md` for the exact structure. The PR body must follow that template. Below are additional filling guidelines only.

## Filling Guidelines

### Issue Number Line

- Use `Close #xxx` when the PR fully resolves the issue.
- Use `ref #xxx` when the PR is related but does not close the issue.
- Multiple issues: `Close #111, Close #222` or `ref #111, ref #222`.

### Check List Rules

- **Remove** items that do not apply (per template comment: "Remove the items that are not applicable").
- At least one test type must remain.
- For "No code" PRs (docs, CI config), remove all other test types.

### Release Note

- Bug fixes and new features need a meaningful release note.
- Style guide: https://pingcap.github.io/tidb-dev-guide/contribute-to-tidb/release-notes-style-guide.html
- If no user-facing change, keep `None.`.

### PR Title Convention

Format: `pkg [, pkg2, pkg3]: what's changed` or `*: what's changed` for broad changes.
