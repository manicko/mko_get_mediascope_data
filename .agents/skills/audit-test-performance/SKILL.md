---
name: audit-test-performance
description: Analyze test suite performance and design an efficient test execution
  strategy using multi-agent research
---

# audit-test-performance

Use this Skill for the process described in its description. This migrates `.kilo/commands/audit/tests/audit-test-performence.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **tech-lead**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$audit-test-performance` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Workflow Outline

- Goal
- Step 1. Baseline Current State
- Step 2. Measure & Profile Execution
- Step 3. Root-Cause Analysis
- Step 4. Test Taxonomy
- Step 5. Research Proven Optimizations
- Step 6. Design Execution Strategy
- Step 7. Produce Optimization Plan
