# Release Process

Release flow is tag-driven and separate from normal operator workflows.

## Prerequisites

1. Worktree is clean.
2. Local quality and contract checks pass.
3. Target version follows semantic versioning: `MAJOR.MINOR.PATCH`.
4. Operator-facing docs reflect the current discovery / growth / maintenance model.

## Local One-Command Release

```bash
scripts/release_local.sh <version>
```

Example:

```bash
scripts/release_local.sh 0.2.0
```

The script performs:

1. clean git tree check
2. release checks:
   - compile
   - capture integrity
   - capture sanitization
   - response contract path validation
   - contract registry parity
   - production profile template baseline validation
3. version bump in:
   - `pyproject.toml`
   - `src/medium_stealth_bot/__init__.py`
4. release commit and annotated tag `v<version>`
5. push commit and tag unless `--no-push` is used

## GitHub Release Workflow

Workflow: `.github/workflows/release.yml`

Triggers:

- push tags `v*.*.*`
- optional `workflow_dispatch`

Workflow outputs:

- reruns release checks
- builds `sdist` and `wheel` with `uv build`
- generates `dist/SHA256SUMS.txt`
- creates a GitHub Release with generated notes and attached artifacts

## Post-Release Checks

1. Confirm the tag appears in Git and GitHub Releases.
2. Confirm assets include wheel, sdist, and checksums.
3. Smoke-check the released tag if needed.
4. Confirm docs and runbook still match the release behavior.

## Bad Release Procedure

If a release is bad:

1. stop schedulers and set `OPERATOR_KILL_SWITCH=true`
2. revert the deployment checkout to the prior stable tag
3. revalidate profile and contracts
4. create a corrective patch release
5. document the incident in [docs/ROLLBACK.md](/Users/moalimir/Project%20World/Medium-Stealth-Bot/docs/ROLLBACK.md)
