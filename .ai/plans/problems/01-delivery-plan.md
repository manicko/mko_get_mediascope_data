---
plan_id: "01-delivery-plan"
source_decisions: "docs/00-overview/open-questions.md"
source_behavior: "docs/00-overview/current-behavior.md"
phase: "post-documentation"
status: draft
agent_role: "tech-lead"
findings_covered: ["G-01", "G-02", "G-03", "G-04", "G-05", "D-06", "D-08", "D-09", "D-10", "D-12", "D-13", "D-14", "D-15", "D-17", "D-19", "D-20", "DISC-01", "DISC-02", "DISC-10", "DISC-11", "R-01", "R-02", "R-03"]
---

# High-Level Delivery Plan — from documentation to implementation

## 1. Overview

Covers every item from the open-questions register that requires **code changes**
or **prior architecture research**. Decision IDs (D-*), gap IDs (G-*), and
research IDs (R-*) refer to
[`docs/00-overview/open-questions.md`](../../docs/00-overview/open-questions.md);
the code-verified current state is in
[`docs/00-overview/current-behavior.md`](../../docs/00-overview/current-behavior.md).

Ground rules (project rules + owner decision D-05):

- Only **one implementor** at a time; at most **2 parallel subagents**; no
  background launches.
- **Research override (owner decision, 2026-10-15):** every research task in
  this plan runs **3 independent researcher agents** on the same brief to
  collect different opinions, and their output is then passed to **one
  validator agent** that checks all three against the code and the decisions
  and selects a single unified solution (see
  [Research pattern](#research-pattern-3-opinions-1-validator)). This exceeds
  the standing "max 2 parallel subagents" rule in `.kilo/rules/commands.md`;
  either the owner raises that limit to 3, or the three researchers run in
  waves of 2 + 1 (opinions stay independent either way).
- The audit corpus is a lead source only — anything an agent claims must be
  verifiable against the code (D-05).
- Every user-visible change ships with tests; documentation is updated in the
  same change (docs-specialist).
- Gates per change: `uv run ruff check <changed paths>`, `uv run pytest`,
  and no **new** mypy errors in touched files (the repo baseline is red:
  56 errors in 7 files — verified 2026-10-15; do not treat as a blocker, track
  as its own cleanup item if desired).

## 2. Work items

| ID | Item | Type | Depends on | Owner decision needed |
|---|---|---|---|---|
| W-01 | Repair all bundled templates: 30 invalid entries → current schema; fix both malformed crosstab defaults (`defaults/tv/crosstab.yaml` line 138, `defaults/tv_reg/crosstab.yaml` line 162); remove stale kitId legend (D-06, D-11, D-20, G-05, DISC-11) | code/config | — | no |
| W-02 | Error semantics overhaul: worker exception isolation; "no data" = warning vs "processing failure" = error file; safe error YAML; missing/partial-period warnings; poll deadline; FAILED/CANCELLED handling (D-08, D-09, D-15, D-17, G-01, G-02) | code | W-02-research | no |
| W-03 | Template-driven region selection; remove hard-coded exclusion of region 99 (D-10, G-03) | code | R-01 | no |
| W-04 | Configurable output conventions: defaults in `app_config.yaml` + per-report override (D-13, G-04) | code | R-02 | no |
| W-05 | Configurable DICT dictionary columns via a user settings file (D-14, G-04) | code | R-03 | no |
| W-06 | Credential setup best practice: final file name, placeholders, `.gitignore`, startup validation (D-12, Q-07, DISC-04) | code/config | — | **yes** — pick from the recorded recommendation |
| W-07 | README corrections: `list-reports` (not `list_reports`), remove `--no-verbose`, install from GitHub per D-19 (DISC-01, DISC-02, DISC-10) | docs | — | partially (install path wording) |
| W-08 | Docs sync after each behavior change (current-behavior, data-flow, product, register) | docs | each item | no |

## 3. Sequence and agents

### Sprint 0 — owner decisions (blocks W-06, part of W-07)

- Owner picks the credential-setup variant for D-12 and confirms the README
  install wording (D-19: no PyPI; `pip install <github_url>`).
- No agent work; 15 minutes of owner time.

### Sprint 1 — quick wins (no research) — parallel: 1 implementor + 1 docs-specialist

| Order | Item | Agents | Notes |
|---|---|---|---|
| 1 | W-01 (+ kitId legend removal folded in) | **implementor** → **validator** → **docs-specialist** | Templates only; no logic change. Done = every bundled entry passes `ReportSettings`, both crosstab defaults parse (`yaml.safe_load`), at least one entry per shipped media/report type runs against a mocked network |
| 1 (parallel) | W-07 | **docs-specialist** | README only; verify claims against the CLI surface (already probe-verified) |

### Sprint 2 — W-02 error semantics (the critical mechanism per D-09)

Pre-work in parallel (2 agents, neither is an implementor):

- **auditor** — verify the current failure-class boundaries against the code
  (before-pool failures, logical failures, in-worker exceptions), confirm the
  probe results, and enumerate every `raise`/`status=False` path in
  `core/reports.py` + `core/network.py`. Output must be code-cited (D-05).
- **3 × researcher** — modern practice for: async worker exception isolation
  with `asyncio.Queue`; durable per-task error artifacts; retry vs. give-up
  policy for a vendor task API; warning channels for "no data" ranges. Three
  independent agents, same brief, no shared context → their proposals then go
  to **validator** (one agent), which checks each against the code and D-05 and
  selects one approach to hand to the planner.

Then:

| Order | Item | Agents | Notes |
|---|---|---|---|
| 2a | Sequence W-02 into atomic slices | **planner** | Uses auditor report + the validator-selected research solution |

| Order | Item | Agents | Notes |
|---|---|---|---|
| 2b | Slice 1: worker try/except → failed-task error artifact, never hang, run continues | **implementor** → **validator** | Test: a raising `send_task`/`wait_task` no longer hangs `queue.join()`; error artifact written; other tasks complete |
| 2c | Slice 2: distinguish "no data" (warning, no error file) from "processing failure" (error file); use the existing `log_error` flag (currently write-only) or an explicit reason field; switch error YAML to `yaml.safe_dump` | **implementor** → **validator** | Test both branches; error YAML re-loadable with `safe_load` |
| 2d | Slice 3: missing/partial-period warnings (zero intervals → warning naming the requested range; partial overlap → warn about the uncovered remainder) | **implementor** → **validator** | Test via mocked availability window (patterns exist in `.ai/tmp/doc_probe_*.py`) |
| 2e | Slice 4 (hardening, optional): poll deadline in `wait_task`; FAILED/CANCELLED → failed task with artifact instead of extraction attempt | **implementor** → **validator** | Only if slices 1-3 are stable; otherwise file as follow-up |
| 2f | Docs sync | **docs-specialist** | Update current-behavior error table, data-flow error rules, product target-behavior status, register gap G-01/G-02 closure |

### Research pattern: 3 opinions, 1 validator

Every research task in this plan (Sprint 2 pre-work patterns, R-01, R-02, R-03)
follows the same three-step flow:

1. **3 × researcher** — the same brief is given to three independent researcher
   agents. They do not share context with each other; each returns its own
   design proposal (mechanism, trade-offs, migration path, risks). The briefs
   must state the constraints they may not violate (D-01 two-layer settings,
   D-04 independent `max_tasks`/semaphore, D-05 code-is-truth, D-08/D-09/D-15/
   D-17 error semantics, existing Strategy/Pydantic patterns, Python 3.12+).
2. **1 × validator** — receives all three proposals plus the code references
   and the decision register. It checks each proposal for: code-traced
   conformance (D-05 — rejects anything not backed by the code), conflict with
   owner decisions, backward compatibility with existing templates and the
   resume mechanism, minimal change / no overengineering, and testability. It
   then selects one solution — or an explicit merge of two — and records the
   rationale: what was rejected and why.
3. Output artifact: the chosen solution is written to
   `.ai/problems/<NN>-<topic>-decision.md` and the register entry in
   `docs/00-overview/open-questions.md` is updated (decision preserved, not
   deleted). Only then does the work proceed to planner/implementor.

Rationale for the pattern (owner's): three independent opinions surface
alternatives a single researcher would miss; the validator gate is what keeps
the result a single, reviewed decision instead of three competing drafts.

### Sprint 3 — research-driven features (3 opinions + validator per item)

| Order | Item | Agents | Notes |
|---|---|---|---|
| 3a | Per research item (R-01, R-02, R-03), run the pattern separately: **3 × researcher** (same brief, independent) → **validator** selects one solution | **researcher ×3 → validator** | Briefs must cite the constraints: D-01 two-layer settings, D-04 independent concurrency knobs, D-05 code-is-truth, D-08/D-09/D-15/D-17 error semantics, backward compatibility with existing templates and the resume mechanism, Pydantic models, Python 3.12+. Output: `.ai/problems/<NN>-<topic>-decision.md` + register update |
| 3b | W-03 regions | **planner** → **implementor** → **validator** → **docs-specialist** | Uses the R-01 decision memo; removes the hard-coded 99 exclusion; `regions` becomes a typed model field |
| 3c | W-04 output conventions | **planner** → **implementor** → **validator** → **docs-specialist** | Uses the R-02 decision memo; backward compatible: defaults reproduce today's `;`/`,`/utf-8-sig/gzip |
| 3d | W-05 DICT columns | **planner** → **implementor** → **validator** → **docs-specialist** | Uses the R-03 decision memo; default settings file reproduces today's hard-coded layout |

### Sprint 4 — leftovers

| Order | Item | Agents | Notes |
|---|---|---|---|
| 4a | W-06 credentials | **implementor** → **validator** → **docs-specialist** | After the owner's D-12 choice; template rename + `.gitignore` + explicit startup error |
| 4b | Optional: mypy baseline cleanup | **implementor** → **validator** | 56 errors in 7 files today; decide whether to gate CI on it |

## 4. Cross-cutting notes

- **Do not couple** `max_tasks` and the HTTP semaphore (D-04) in any of the
  above changes.
- W-02 must preserve the resume contract (D-09): a re-run skips existing files
  and re-submits nothing that is already written; add a test per slice.
- Every behavior change updates the docs in the same change (W-08) — the
  register keeps decisions, not duplicated specs.
- If an agent's output contradicts the code, the code wins and the agent is
  re-run with the correction (D-05).
- No research output enters implementation until it has passed the
  3-opinions → validator gate and is recorded as a decision memo; the planner
  works only from the selected solution.

## 5. Definition of done (per item)

1. Change is minimal and follows existing patterns (Strategy composition,
   Pydantic models, module-level loggers).
2. Tests cover the user-visible behavior; `uv run pytest` green; ruff clean on
   touched paths; no new mypy errors in touched files.
3. Docs updated: current state, data flow if data handling changed, register
   entry closed or updated with rationale.
4. No unrelated changes in the diff.
