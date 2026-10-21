---
name: audit-phases-review
description: Assess whether the current audit phases adequately cover the architecture
  and determine the minimum required new audit phases
---

# audit-phases-review

Use this Skill for the process described in its description. This migrates `.kilo/commands/audit/audit-phase-gaps.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **tech-lead**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$audit-phases-review` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

Keep four independent researcher perspectives: run two batches of two (or serially), never four concurrently. Later researchers receive the same original context, not earlier opinions. The source step says lowest unused number excluding 99, while its final paragraph says above all existing phases: use the explicit lowest-unused allocation rule and record the choice. Create any approved new phase only as a new .agents/skills/NN-phase-name/SKILL.md plus references/workflow.md, then update .codex/audit-phases.md. Never edit .kilo/.

## Workflow Outline

- Step 1. Study Current Architecture and Existing Audit Phases
- Step 2. Independent Research
- Step 3. Validate & Consolidate
- Step 4. Create New Phase Specifications (conditional)
- Step 5. Final Document Audit
