# Release Process

Release flow is tag-driven and separate from normal quality checks.

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
   - capture sanitization check
   - response contract path check
   - tests
   - contract registry parity
   - production profile template validation
3. version bump in:
   - `pyproject.toml`
   - `src/medium_stealth_bot/__init__.py`
4. release commit + annotated tag `v<version>`
5. push commit and tag (unless `--no-push`)

## GitHub Release Workflow

Workflow: `.github/workflows/release.yml`

Triggers:

- push tags `v*.*.*`
- `workflow_dispatch` (optional manual tag input)

Workflow outputs:

- re-runs release checks
- builds `sdist` and `wheel` with `uv build`
- generates `dist/SHA256SUMS.txt`
- creates GitHub Release with generated notes and attached artifacts

## Rollback of a Bad Release

If a release is bad:

1. stop schedulers and set `OPERATOR_KILL_SWITCH=true`
2. revert to prior stable tag in deployment environment
3. create a corrective patch release
4. document incident in `docs/ROLLBACK.md` record section
