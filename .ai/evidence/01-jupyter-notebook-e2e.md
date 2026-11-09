# Jupyter Notebook E2E Evidence — Mediascope workflow (CR-5)

> **Status: BLOCKED (not executed in this implementation session)**

This note is a sanitized evidence template for the repo-shipped notebook
`examples/mediascope_notebook_workflow.ipynb` (B3 / OQ-3 Alternative A, internal
DI workspace bootstrap). It must **not** contain credentials, tokens, usernames,
passwords, client secrets, auth secrets, raw API payloads, or other sensitive
data.

## Delivery context

- **Selected Git ref (notebook default, pinned):** `0743c3d21ed5ac6291ab0a96dfab26db0664c05f`
  - Repo has no tags at delivery time; notebook keeps a commented branch override
    (for example `main`) as a non-default option.
- **Repo:** `https://github.com/manicko/mko_get_mediascope_data.git`
- **Notebook:** `examples/mediascope_notebook_workflow.ipynb`
- **Date (implementation):** 2026-11-09
- **Environment (this session):** implementation workspace only; **no authorized
  local JupyterLab/classic run executed here**.

## Execution status

- **Result:** BLOCKED — no success claim.
- **Reason:** Real Mediascope credentials and network/API access were not
  available to the implementor in this session. CR-5 (install from Git →
  configure folders/credentials → run an actual Mediascope report → confirm
  output files) requires an authorized local JupyterLab/classic run with valid,
  user-managed external credentials.
- **What was verified in this session:** notebook JSON/nbformat validity, static
  contract checks (no `nest_asyncio`, no `run_report_async` singleton wrapper in
  the Alt A execution path, `NullProgressReporter()` used, `await
  service.run_report(...)` present), and targeted pytest sanity. This is
  contract/static verification only — **not** real end-to-end execution.

## Configured variables (names only; values redacted where sensitive)

Record the actual values used in an authorized run **except** credential paths
that reveal private locations if you choose to redact them, and never record
credential contents.

- `REPO_URL`: `https://github.com/manicko/mko_get_mediascope_data.git`
- `GIT_REF`: `0743c3d21ed5ac6291ab0a96dfab26db0664c05f` (pinned default)
- `WORKSPACE_ROOT`: *(fill in authorized run)*
- `SETTINGS_WORKSPACE`: *(fill in authorized run)*
- `REPORTS_DIR`: *(fill in authorized run)*
- `REPORT_FILENAME` / `REPORT_PATH`: default workspace-local
  `nat_tv_brands_theiss.json5` (OQ-2); override allowed
- `EXPORT_DIR`: *(fill in authorized run; notebook sets `app_config.json5`
  `export_folder` to absolute `str(EXPORT_DIR)` before `AppService` construction)*
- `CREDENTIALS_PATH`: external/user-managed file outside notebook content
  (portable user-config default; Windows example in notebook comments only).
  **Do not record secret values.**
- `COPY_CREDENTIALS_INTO_WORKSPACE`: explicit opt-in (default `False`)

## Output verification (authorized run)

- **Export paths returned by `run_report`:** N/A (blocked)
- **Output file listing/count/sizes under export:** N/A (blocked)
- **Notes:** Verification must account for `relative_path` under export and that
  report entries may be skipped (`NoReportFoundError` warned) — do not claim
  success from path existence alone.

## Redaction / exclusion checklist

Before sharing this note or notebook outputs, confirm all are excluded:

- [ ] Credentials file contents (`mediascope.json` values)
- [ ] Usernames, passwords, client IDs/secrets, tokens, auth secrets
- [ ] Raw Mediascope API requests/responses/payloads
- [ ] Notebook cell outputs containing sensitive paths or secrets (clear outputs)

## Re-run instructions (authorized environment)

1. Run in local JupyterLab/classic Jupyter with Python 3.12+ and network access.
2. Provide valid external credentials at `CREDENTIALS_PATH` (or copy in via
   explicit opt-in). Do not place secrets in notebook cells.
3. Execute cells in order; restart kernel after install cell if dependencies
   changed.
4. If execution reaches CR-5, update status to **SUCCESS** or **PARTIAL/BLOCKED**
   with a non-sensitive reason, fill configured paths (redacted as needed),
   and record output listing/count/sizes only.