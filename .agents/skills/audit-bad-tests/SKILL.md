---
name: audit-bad-tests
description: Audit test quality, identify obsolete or low-value tests, and recommend
  test improvements or missing coverage using multi-agent research
---

# audit-bad-tests

Use this Skill for the process described in its description. This migrates `.kilo/commands/audit/tests/audit-bad-tests.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **tech-lead**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$audit-bad-tests` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Workflow Outline

- Goal
- Step 0. Verify Test Environment
- Step 1. Analyze Current Architecture, Test Conventions and Partition Tests
- Step 2. Audit Test Quality (per block)
- Step 3. Validate Findings (per block)
- Step 4. Produce the Audit Report
- Step 5. Create the Implementation Plan
