---
name: audit-tests-full
description: Audit the complete Python test suite against actual architecture, meaningful
  behavior and missing coverage; produce recommendations without editing tests.
---

# audit-tests-full

Use this Skill for the process described in its description. This migrates `.kilo/commands/audit/tests/audit-tests-full.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **auditor**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$audit-tests-full` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

This is an audit deliverable. The source objective says delete/rewrite, but its assigned auditor role prohibits test edits: report TEST-DELETE/TEST-REWRITE recommendations; do not execute them. Separate an actual intended-contract defect from an obsolete test; production-first does not authorize silently ignoring a valid requirement.

## Workflow Outline

- Step 0 — Test Environment
- Objective
- Recommendation Types
- Research
- Anti-Patterns (subject to deletion or complete rewrite)
- Additional Checks
- pytest Standards
- System Coverage
- Audit Result Format
- Acceptance Criteria
