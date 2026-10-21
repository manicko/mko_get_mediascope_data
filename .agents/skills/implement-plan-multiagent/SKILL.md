---
name: implement-plan-multiagent
description: Execute development plan incrementally using adaptive multi-agent workflow
---

# implement-plan-multiagent

Use this Skill for the process described in its description. This migrates `.kilo/commands/implement/implement-plan-multiagent.md`; the original remains unchanged.

## Execution Contract

- Executor/coordinator: **tech-lead**. This is routing guidance, not a hard dependency for tasks whose source permits other executors.
- Treat text, paths, selections and debug/plan context after `$implement-plan-multiagent` as task input. There is no Kilo slash-command or automatic positional-variable expansion. Bind the workflow's `{variables}` to the supplied input and generated artifacts. Request only required missing input; preserve user selections and pending approval gates.
- Read `AGENTS.md`; use its verification commands. Follow its two-subagent and single-implementor budgets, wait for delegated results, and respect `.kilo/` preservation.
- Before doing the task, read [the complete workflow](references/workflow.md) in full. It is mandatory procedure, including every stage, branch, report format, retry and stop condition. Paths in that document resolve from the repository root.
- For audit phases, use the phase metadata below and `.codex/templates/audit-findings.md`. Static proof is admissible where the phase permits it; label it accurately and do not claim a runtime observation. Treat historical implementation statements as hypotheses to verify.
- Preserve partial artifacts on errors. Never report a missing output, blocked required approval, unverified behavior, or incomplete independent review as success.

## Codex Adaptation

Input is plan_file (an existing path) plus optional scope. Resolve or request that input before execution. For every high-risk block, run validator after planner and before implementor, as required by the source risk selection but omitted from its numbered executor list. On final validation findings, fix with one implementor and revalidate before acceptance. When the input plan is already sufficient, use it as the execution plan or create the next-number plan if bookkeeping is needed. Commits belong only to this invoked, user-authorized implementation workflow, never to this migration itself.

## Workflow Outline

- 0. Inspect relevant current architecture and implementation 
- 1. Decompose Plan
- 2. Ask the user as the Product Owner
- 3. Execute Blocks
- 4. Documentation
- 5. Final Validation
- 6. If issues are found:
