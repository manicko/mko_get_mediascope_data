---
name: doc-architecture
description: Explore the Project and Create Product and Architecture Documentation
---

# doc-architecture

Use this Skill for the process described in its description. This migrates `.kilo/commands/docs/doc-architecture.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **Main agent or docs-specialist**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$doc-architecture` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

The suggested uppercase root docs are examples: reuse this repository's existing docs/00-overview/ product-overview.md, current-behavior.md, data-flow-architecture.md, system-architecture.md and open-questions.md. Preserve all investigation stages and owner-decision gates; follow the existing lowercase, frontmatter and no-duplication documentation rules.

## Workflow Outline

- 1. Objective
- 2. Constraints
- 3. Investigation Principles
- 4. Work Stages
- 5. Recommended Deliverables
- 6. Documentation Quality Requirements
- 7. Acceptance Criteria
