# Releasing rannoch

This is the procedure an agent follows to release rannoch. Run the steps in
order. On the first failure, stop and report it as the Failure section says.

A release is a git tag and a GitHub release. rannoch is not published to
crates.io yet (`publish = false`), so consumers pin the tag:

```toml
rannoch = { git = "https://github.com/runlegion/rannoch", tag = "vX.Y.Z" }
```

Config is `release.toml`. The mechanics are `scripts/release.sh` and
`scripts/sync-version.sh`, copied from rafters-studio/migratr, which copied them
from runlegion/legion (legion 0.43.5) with two patches: the version bump also
moves the `version` pin on every `path = "..."` dependency, and `--activate` is
refused. Check their behavior with `bash scripts/test-release.sh`.

Replace `X.Y.Z` with the new version, `<level>` with `patch`, `minor` or
`major`, and `<pr>` with the release PR number.

## Keeping state

A release can outlive one agent session. After every step that creates something
(the release issue, the release PR, the tag), comment it on the release issue. To
resume, read that issue's comments, check what exists, and continue at the first
step not done.

## 1. Preconditions

On `main`, in sync with origin, no uncommitted changes to tracked files.
Untracked files are ignored, as `release.sh`'s own guard ignores them.

```bash
git rev-parse --abbrev-ref HEAD                      # main
git fetch origin main && git status -sb              # no "ahead"/"behind"
git status --porcelain --untracked-files=no          # empty, or only CHANGELOG.md when resuming after step 2
git branch --list 'release/*'                        # empty
```

If any fails, stop and report which. Change nothing.

## 2. Changelog

Dispatch the `legion:changelog` agent for rannoch. It prepends `## X.Y.Z` to
`CHANGELOG.md` with a line `<Patch|Minor|Major> release: <rationale>` as the
first paragraph under the heading. Read the result back: `X.Y.Z` is the first
`## X.Y.Z` heading, and `<level>` is the word before `release:`, lowercased. If
the agent failed, or the entry has no such heading and line, stop and report it.

While rannoch is 0.x, a change to a public signature is a minor release.

Leave the entry uncommitted. `release.sh` takes it into the release commit.

## 3. Consumer check (FR-RANNOCH-054)

Before each release, run the rannoch-using tests of legion and eavesdrop
against the release candidate (current `main`), and record the result on the
release issue in step 4. A failure in either holds the release: stop and report
it.

When an app does not depend on rannoch yet, there is nothing to run for it;
record that, naming the app and the commit checked.

## 4. Release issue

```bash
legion issue create --repo rannoch --title "chore(release): vX.Y.Z" \
  --body "$(printf '## Acceptance criteria\n\n- rannoch X.Y.Z is tagged vX.Y.Z and on a GitHub release whose notes are the X.Y.Z CHANGELOG entry\n- The consumer check (FR-RANNOCH-054) is recorded on this issue\n')"
```

The pr-write gate needs an issue to map. Note the issue number, and comment the
step 3 result on it.

## 5. Stage the release commit

```bash
scripts/release.sh <level>
```

It runs the preflight commands from `release.toml`, bumps `Cargo.toml`, refreshes
`Cargo.lock`, commits `chore(release): X.Y.Z` on `release/X.Y.Z`, and pushes that
branch. It refuses when the `## X.Y.Z` heading does not equal the version it
computes from `<level>`; stop and report its message.

## 6. Gates, PR, merge

On `release/X.Y.Z`, run `/legion:legion-simplify`, then `/legion:legion-pr-write`
(both are keyed to the release commit). pr-write runs
`legion pr write-check --repo rannoch --issue <issue>` on a body file kept
outside the repo that maps the release issue's criteria. Then open and merge:

```bash
legion pr create --repo rannoch --title "chore(release): vX.Y.Z" --head release/X.Y.Z --closes <issue> --body "$(cat BODY)"
legion pr merge --repo rannoch --number <pr> --strategy merge
```

## 7. Tag

```bash
scripts/release.sh X.Y.Z --finish=<pr>
```

It waits for the merge, tags the merged release commit, and pushes the tag. The
tag runs `release.yml`: it checks the tag against `Cargo.toml`, builds, and
creates the GitHub release with the X.Y.Z CHANGELOG entry as its notes.

## 8. Wait for the GitHub release

Poll up to 60 times, 20 seconds apart, until the latest release is `vX.Y.Z`.
This uses `gh`, not `curl`: legion's hook stops `curl` for operator approval.

```bash
for i in $(seq 60); do
  tag="$(gh release view --repo runlegion/rannoch --json tagName --jq .tagName 2>/dev/null)"
  [ "$tag" = "vX.Y.Z" ] && break
  sleep 20
done
[ "$tag" = "vX.Y.Z" ] || { echo "release.yml has not published vX.Y.Z"; gh run list --repo runlegion/rannoch --workflow release.yml --limit 1 --json url --jq '.[0].url'; exit 1; }
```

## 9. Report

Report the version, the release PR, the tagged sha, the GitHub release URL, and
the consumer check result.

## Failure

- Step 1 fails: stop and report which precondition. Nothing has changed.
- The consumer check fails: stop and report the failing app and test. Nothing
  has changed.
- The `## X.Y.Z` heading disagrees with the version `release.sh` computes from
  `<level>`: it refuses at its header check. Stop and report both versions.
- A release-side gate fails (simplify or pr-write in step 6): stop and report the
  gate's output, the release issue and the pushed `release/X.Y.Z` branch.
  `release.sh` refuses to stage again while that branch exists, so a later run
  first deletes the unmerged branch and reuses or closes the issue.
- Merge or tag failures: report `release.sh`'s message verbatim and stop.
- Step 8 times out: report "release.yml has not published vX.Y.Z" with the
  Actions run URL.
