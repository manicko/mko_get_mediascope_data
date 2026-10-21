---
name: doc-implementation-update
description: Update project documentation based on implemented significant functionality
  using multi-agent research
---

# doc-implementation-update

Use this Skill for the process described in its description. This migrates `.kilo/commands/docs/doc-implementation-update.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **docs-specialist**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$doc-implementation-update` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

Research delegation is mandatory for each large-context step. If docs-specialist is itself a child and no slot remains, ask the parent to run researcher and return its summary rather than bypassing the global budget. The discrepancy-report directory is .ai/audit/problems/; application code stays unchanged.

## Workflow Outline

- Goal
- Step 1. Launch Researchers – Identify Significant Functionality from Specifications / Plans
- Step 2. Launch Researchers – Verify Current Implementation & Architecture
- Step 3. Launch Researchers – Determine Documentation Impact & Required Updates
- Step 4. Implementation Discrepancy Reporting
- Step 5. Apply the Updates (docs-specialist only)
