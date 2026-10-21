---
name: audit-phases-refinement
description: Refine existing audit phases to match the current architecture while
  preserving their original intent
---

# audit-phases-refinement

Use this Skill for the process described in its description. This migrates `.kilo/commands/audit/audit-phase-refine.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **tech-lead**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$audit-phases-refinement` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

The registry replaces the source phase directory. Refinement edits only the selected Codex Skill's references/workflow.md (and its SKILL.md only if routing metadata must stay consistent). Never edit the Kilo counterpart. Final adjustments consume Step 2's report, correcting the source's Step 5 reference.

## Workflow Outline

- Step 0. Phase Selection
- Step 1. Execute phase loop
- Step 2. Final Consistency Check (after all selected phases)
- Step 3. Final Adjustments
- Goal
