# Multi-Agent Audit Pipeline

## 0. Select Phases

Read `.codex/audit-phases.md` and present a numbered table of content phases,
excluding `99-validate`. Accept `all` or comma-separated phase numbers (for example
`1,3,4`). Use a selection already supplied by the user; otherwise ask and wait.
Record `SELECTED_PHASES` in the confirmed order. Do not fill numbering gaps.

## 1. Gather Base Context Once

Read the verification commands in `AGENTS.md` and list the `docs/` structure.
Set `BASE_CONTEXT` to this summary and `REPORT_TEMPLATE_PATH` to
`.codex/templates/audit-findings.md`. The coordinator does not read production
code, full documentation, executor roles/tasks or templates here: send their paths.

## 2. Execute Each Selected Phase

Run the phase loop in selection order. Follow the global two-subagent budget,
wait for results, and resume existing sessions after interruptions.

### 2.1. Resolve Metadata

Resolve the phase number, name, Skill and workflow path through the registry.
Set `OUTPUT_PATH` to `.ai/audit/{NN}-{phase-name}/findings.md`.

### 2.2. Delegate to Auditor

Use the host's native delegation tool with the registered `auditor` role.
Send this task verbatim with resolved values:

```text
Read the selected phase SKILL.md and execute its references/workflow.md.
Write findings to: {OUTPUT_PATH}
Report template: {REPORT_TEMPLATE_PATH}
Base context: {BASE_CONTEXT}
problems_only = TRUE
Important: Never change any code files.
```

### 2.3. Verify Auditor Output

Wait for completion. Verify the expected file exists and is nonempty. A missing
or empty output gets one retry in the same session. On a second failure, stop
that phase and present a structured failure report (phase, error, missing output).
Do not proceed as if the phase succeeded.

### 2.4. Conditional Independent Validation

Use `validator` only for nontrivial findings concerning significant code changes
or architectural changes; otherwise record validation as skipped with its reason.
Supply the `99-validate` Skill and workflow, raw `OUTPUT_PATH`, report template,
`BASE_CONTEXT`, `problems_only = TRUE`, and the prohibition on code changes.
The required output is exactly:
`.ai/audit/99-validation/{NN}-{phase-name}-validated-findings.md`.

### 2.5. Verify Validator Output

Wait and verify that exact output path exists and is nonempty. Retry a missing
or empty output once, then escalate on the second failure. Preserve the audited
finding IDs and a disposition for every finding. Rejected findings are excluded
from any merged actionable set, but remain visible in the disposition record;
never delete or edit the raw report from a per-phase validation.

## Retry and Stop Rules

- At most two retries (three attempts) for a phase as an absolute ceiling.
- The output-existence gates are stricter: one retry, escalation on the second
  failed attempt. The general ceiling does not authorize a third silent attempt.
- Keep completed artifacts and report partial completion on failure.
- Missing delegation is a blocker for independent validation; disclose it and
  do not label a same-agent check independent.

## Final Result

```text
AUDIT COMPLETE (or PARTIAL if any required phase failed)
Phases completed: {completed}/{selected}
Validated findings: {total}
CRITICAL: {n}
HIGH: {n}
MEDIUM: {n}
LOW: {n}
```

Include raw and validated report paths, skipped validations with reasons,
unsettled/rejected dispositions, retries, and any blocked phase. Count only
actually completed and validated work.
