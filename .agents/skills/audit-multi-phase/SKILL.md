---
name: audit-multi-phase
description: Execute full multi-agent audit pipeline using orchestrator coordination,
  executor subagents, and validator subagents with retry logic
---

# audit-multi-phase

Use this Skill for the process described in its description. This migrates `.kilo/commands/audit/audit-multi-phase.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **Main agent or tech-lead**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$audit-multi-phase` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Workflow Outline

- 0. Select Phases
- 1. Gather Base Context Once
- 2. Execute Each Selected Phase
- Retry and Stop Rules
- Final Result
