---
phase: "13"
phase_name: "performance"
id_prefix: "PERF"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".agents/skills/13-performance/references/workflow.md#severity-taxonomy"
code_ref: "b27b8708d4d816940310d4f034b549cb1db6f397"
---

# Audit Findings — Performance

## Summary

One LOW documentation problem misstates which default polling limit stops a waiting task. Bounded synthetic checks did not establish a new throughput, starvation or memory-exhaustion defect. Production capacity and real API latency remain unmeasured.

## Scope

**Examined:** all six blocks: concurrency tiers, per-task call cost, retry cost, worker/queue pressure, data retention and observable latency. Full coverage and bounded measurement provenance are in `.ai/tmp/audit-2026-10-21/phase13-coverage.md`.
**Tools:** static source/config reads, `rg`, Git inventory, bounded existing-venv Python/pandas probe. No broad gate rerun or new dependencies.
**Snapshot:** current working tree, with code ref as provenance only; hashes of core modules and bundled app settings are recorded in `phase13-probe-output.json`. Pre-existing external changes, including `.kilo`, remain untouched. The cited comment was rechecked after the probe.
**Not verified:** production-volume/API-load latency, actual remote polling outcomes, thread saturation, native event-loop scheduling, process RSS/OOM, or a universal throughput target. Synthetic-clock seconds below are arithmetic through real application methods with patched clock/sleep/invocation, not elapsed wall time.
**ID provenance:** historical PERF-001–003 remain reserved, including the workflow-noted ambiguous prior use of PERF-001. No current or preserved prior PERF-004 was found; this report does not reinterpret historical claims.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| PERF-004 | LOW | `src/mko_get_mediascope_data/settings/app_config.json5::"With defaults, max_polls trips first"` | Poll-budget guidance excludes per-call jitter |

#### PERF-004 — [LOW] Poll-budget guidance excludes per-call jitter

**Anchor:** `src/mko_get_mediascope_data/settings/app_config.json5::"With defaults, max_polls trips first"`
**Anchor:** `src/mko_get_mediascope_data/core/reports.py::TVMediaReport.wait_task`
**Anchor:** `src/mko_get_mediascope_data/core/network.py::NetworkClient.call`
**Disambiguator:** `~3560s < 3600s deadline`
**Checked:** ran (synthetic clock, not native wall-clock performance)
**Type:** [DOC-UPDATE]
**Why this band:** a comment no longer describes its adjacent default behavior. This does not demonstrate harmful production cost or a failed runtime guard.
**Problem:** default-budget guidance sums the outer poll intervals but excludes the additional 15–60 seconds of initial network smoothing before each status request. It consequently predicts the counter guard wins over the deadline.
**Repro:** use current bundled settings, real `TVMediaReport.wait_task` and `NetworkClient.call`, immediate synthetic RUNNING responses, and a clock advanced by requested sleeps. At minimum/midpoint/maximum jitter, the deadline stops polling after 49/38/31 calls, respectively, before the configured 60-call counter. Exact output is below.
**Impact:** maintainers receive an incorrect model when tuning polling limits. No runtime behavior needs changing to correct this comment.
**Root cause:** documented duration accounts for only one of the two sequential sleep mechanisms.
**Fix:** correct `app_config.json5::"With defaults, max_polls trips first"` to account for both sleeps and distinguish a pre-iteration deadline check from a strict wall-clock cutoff. Benefit: accurate tuning guidance; change size: trivial; advisory priority: low.
**Verify:** new test needed: test_documented_default_poll_budget_includes_network_smoothing
**Related:** ASY-009 and EXT-009–011 retain their timeout/concurrency and integration ownership; this finding requests only a comment correction.

**Evidence — bounded probe:** `.\.venv\Scripts\python.exe -u -X utf8 .ai/tmp/audit-2026-10-21/perf_probe.py` exited 0 in 1.984s. It saved full exact JSON in scratch. Exact relevant output projection, obtained with `Get-Content .ai/tmp/audit-2026-10-21/phase13-probe-output.json | ConvertFrom-Json | Select-Object -ExpandProperty default_stuck_poll_simulations | ConvertTo-Json`:
```json
[
  {
    "jitter_fraction": 0,
    "poll_calls": 49,
    "synthetic_seconds": 3635.0,
    "error": "Poll deadline exceeded (3600.0s)"
  },
  {
    "jitter_fraction": 0.5,
    "poll_calls": 38,
    "synthetic_seconds": 3665.0,
    "error": "Poll deadline exceeded (3600.0s)"
  },
  {
    "jitter_fraction": 1,
    "poll_calls": 31,
    "synthetic_seconds": 3680.0,
    "error": "Poll deadline exceeded (3600.0s)"
  }
]
```

## Fix Order

- **Dependencies / merge candidates / conflicting evidence:** None.
- **Cross-phase conflicts:** no refile of ASY-008/009/010, rejected ASY-011, EXT-010 session cleanup or existing pipeline defects. No measured harmful threshold or breached bounded-generation contract supports reviving eager-materialization claims.
- **Rollout risk:** comment-only change; preserve existing polling, deadline and jitter semantics. Independent validation may be skipped for this trivial documentation-only advisory; it is not a confirmed phase99 disposition.

## Appendices

The six-block evidence inventory, measurement environment, arithmetic bounds, frame sizes/timings and explicit unverified limits are retained in `.ai/tmp/audit-2026-10-21/phase13-coverage.md`. Exact probe inputs/code and full JSON are `perf_probe.py` and `phase13-probe-output.json` in the same scratch directory. These artifacts measure local synthetic work, not actual Mediascope capacity.
