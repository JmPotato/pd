# PD Pull Request Template

Source: `.github/pull_request_template.md`

## Template Structure

The PR body must follow this exact structure:

```markdown
### What problem does this PR solve?

Issue Number: Close #xxx

<description of the problem>

### What is changed and how does it work?

<description of the changes and implementation approach>

```commit-message
<concise commit message for squash-merge>
```

### Check List

Tests

- Unit test
- Integration test
- Manual test (add detailed scripts or steps below)
- No code

Code changes

- Has the configuration change
- Has HTTP APIs changed (Don't forget to [add the declarative for the new API](https://github.com/tikv/pd/blob/master/docs/development.md#updating-api-documentation))
- Has persistent data change

Side effects

- Possible performance regression
- Increased code complexity
- Breaking backward compatibility

Related changes

- PR to update [`pingcap/docs`](https://github.com/pingcap/docs)/[`pingcap/docs-cn`](https://github.com/pingcap/docs-cn):
- PR to update [`pingcap/tiup`](https://github.com/pingcap/tiup):
- Need to cherry-pick to the release branch

### Release note

```release-note
None.
```
```

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
