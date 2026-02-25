## Summary

- What changed?
- Why was it needed?

## Validation

- [ ] `uv run python -m compileall -q src`
- [ ] `uv run python scripts/check_capture_integrity.py`
- [ ] `uv run python scripts/check_capture_sanitization.py`
- [ ] `uv run python scripts/check_response_contract_paths.py`
- [ ] `uv run pytest -q`
- [ ] `uv run bot contracts --tag programming --no-execute-reads`

## Risk and Rollback

- Risk level:
- Rollback strategy:

## Docs

- [ ] README updated (if user-facing behavior changed)
- [ ] Project/development docs updated (if operational behavior changed)
