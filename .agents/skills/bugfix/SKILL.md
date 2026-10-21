---
name: bugfix
description: Execute the next bugfix safely and incrementally following project standards
  and architecture constraints
---

# bugfix

Use this Skill for the process described in its description. This migrates `.kilo/commands/debug/bugfix.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **Main agent (missing source debug role)**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$bugfix` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

The source requests a debug role that does not exist. Keep coordination in the main agent and delegate to the existing researcher/planner/validator/implementor roles as specified; do not invent debug. Debug information is the text after this Skill invocation or an explicit file path. Python test syntax is uv run pytest <relevant_path>; frontend checks apply only if a separate actual frontend exists. Preserve the source Git prohibitions and its explicit-user-yes exception; a host approval is also required when applicable.

## Workflow Outline

- Step 1 — Study the Bug
- Step 2 — Preparation (Researcher)
- Step 3 — Select Best Route (if multiple options)
- Step 4 — Risk & Complexity Assessment (Researcher)
- Step 5 — Implementation (Implementor)
