---
id: codex-migration
domain: overview
tags:
  - codex
  - agent-configuration
  - migration
related:
  - doc-maintenance-rules
  - system-architecture
---

# Kilo Code to Codex Configuration Migration

## Purpose

Preserve the existing Kilo Code development system while making its roles,
workflows and project rules available to Codex. This migration changes AI
configuration and this report only. The entire `.kilo/` tree remains available
for parallel use. No application source, tests, dependencies, packaging,
deployment or CI settings were changed, and no implementation workflow was run.

Verification date: **2026-10-21**. Installed executable: **Codex CLI
0.163.0-alpha.10.2**, bundled with the desktop app. There was no project
`.codex/` or `.agents/` configuration to merge before this migration. The existing
root `AGENTS.md` was extended; its project context was retained. User/global
settings were not edited.

Inventory: **9 roles, 29 commands, 2 rule files, 6 existing supporting Skills**.
The resulting catalog has **35 Skills**, including **13 content audit phases
and 1 validation phase**. All **54 files** under `.kilo/`, including licenses,
scripts, the component archive and `.gitignore`, appear in the mapping below.

This report is placed directly under `docs/` because the migration request
explicitly specified `docs/codex-migration.md`; its frontmatter and content
otherwise follow the existing documentation maintenance rules.

## Main Concepts

| Layer | Location | Responsibility |
|---|---|---|
| Project rules | [AGENTS.md](../AGENTS.md) | Persistent architecture, toolchain, language, quality and coordination constraints |
| Executors | [.codex/agents/](../.codex/agents/) | Role specialization, decision criteria, scope, output and completion |
| Workflows | [.agents/skills/](../.agents/skills/) | One Skill per source command; reusable task procedures |
| Scheduling configuration | [.codex/config.toml](../.codex/config.toml) | Native agent enablement and maximum two concurrent subagent threads |
| Role permission reference | [agent-permissions.md](../.codex/agent-permissions.md) | One shared matrix preserving every source permission entry |
| Phase registry | [audit-phases.md](../.codex/audit-phases.md) | Phase selection, executor and authoritative severity rubric |
| Codex report template | [audit-findings.md](../.codex/templates/audit-findings.md) | Adapted shared template; the original `.ai/` template is untouched |

The [official subagent documentation](https://learn.chatgpt.com/docs/agent-configuration/subagents)
specifies standalone `.codex/agents/*.toml` files with `name`, `description` and
`developer_instructions`. The old `[agents.role].config_file` registration
pattern is not used. Model/provider settings are inherited rather than pinned.
The [configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference)
documents `agents.enabled` and `agents.max_concurrent_threads_per_session`.
Installed `config/read` accepted these settings and reported the project layer.

The [official Skills guide](https://learn.chatgpt.com/docs/build-skills) documents
repository discovery under `.agents/skills/`, YAML `name` and `description`, and
explicit `$skill-name` invocation. The
[AGENTS.md guide](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
explains root-to-working-directory instruction discovery. No Kilo `alwaysApply`,
permission YAML, model-provider setting or invented TOML property was used to
imitate a native Codex feature.

## Resulting Structure

```text
AGENTS.md                            existing project rules, extended
.codex/
  config.toml                        agent settings only
  agents/
    analyst.toml
    auditor.toml
    docs-specialist.toml
    implementor.toml
    planner.toml
    researcher.toml
    tech-lead.toml
    test-engineer.toml
    validator.toml
  agent-permissions.md               shared source permission matrix
  audit-phases.md                    13 content phases + phase 99
  templates/audit-findings.md
.agents/skills/
  <29 command names>/
    SKILL.md                         invocation, input, routing, gates
    references/workflow.md           complete adapted source procedure
  <6 supporting Skill names>/
    SKILL.md
    <existing scripts, archive and license files, where supplied>
docs/codex-migration.md
.kilo/                               all original files unchanged
```

Each command's detailed procedure lives once in `references/workflow.md`.
Its `SKILL.md` requires reading that reference in full before execution and
preserves its outline and any command-specific adaptation. Shared scheduling
rules live in `AGENTS.md`; role-specific restrictions live in the shared matrix
and relevant executor. There are no new orchestration roles or wrapper Skills.

## Complete Source Mapping

The source's declared `name` is the callable Skill name; when it differs from
the command filename, neither is silently renamed. The target entrypoint links
to its complete procedure. Resource copies retain their original bytes.

| Source file | Type | Target file | Transfer method |
|---|---|---|---|
| [`.kilo/.gitignore`](../.kilo/.gitignore) | Kilo-only metadata | [`.kilo/.gitignore`](../.kilo/.gitignore) | Kilo-specific ignore rules do not belong in Codex configuration; original unchanged |
| [`.kilo/agents/analyst.md`](../.kilo/agents/analyst.md) | Role | [`.codex/agents/analyst.toml`](../.codex/agents/analyst.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/auditor.md`](../.kilo/agents/auditor.md) | Role | [`.codex/agents/auditor.toml`](../.codex/agents/auditor.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/docs-specialist.md`](../.kilo/agents/docs-specialist.md) | Role | [`.codex/agents/docs-specialist.toml`](../.codex/agents/docs-specialist.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/implementor.md`](../.kilo/agents/implementor.md) | Role | [`.codex/agents/implementor.toml`](../.codex/agents/implementor.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/planner.md`](../.kilo/agents/planner.md) | Role | [`.codex/agents/planner.toml`](../.codex/agents/planner.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/researcher.md`](../.kilo/agents/researcher.md) | Role | [`.codex/agents/researcher.toml`](../.codex/agents/researcher.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/tech-lead.md`](../.kilo/agents/tech-lead.md) | Role | [`.codex/agents/tech-lead.toml`](../.codex/agents/tech-lead.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/test-engineer.md`](../.kilo/agents/test-engineer.md) | Role | [`.codex/agents/test-engineer.toml`](../.codex/agents/test-engineer.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/agents/validator.md`](../.kilo/agents/validator.md) | Role | [`.codex/agents/validator.toml`](../.codex/agents/validator.toml) | Preserved role body; explicit scope/output contract; shared behavioral permission matrix |
| [`.kilo/commands/audit/audit-multi-phase.md`](../.kilo/commands/audit/audit-multi-phase.md) | Command | [`.agents/skills/audit-multi-phase/SKILL.md`](../.agents/skills/audit-multi-phase/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/audit-orphaned.md`](../.kilo/commands/audit/audit-orphaned.md) | Command | [`.agents/skills/audit-orphaned/SKILL.md`](../.agents/skills/audit-orphaned/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/audit-phase-gaps.md`](../.kilo/commands/audit/audit-phase-gaps.md) | Command | [`.agents/skills/audit-phases-review/SKILL.md`](../.agents/skills/audit-phases-review/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/audit-phase-refine.md`](../.kilo/commands/audit/audit-phase-refine.md) | Command | [`.agents/skills/audit-phases-refinement/SKILL.md`](../.agents/skills/audit-phases-refinement/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/audit-refine.md`](../.kilo/commands/audit/audit-refine.md) | Command | [`.agents/skills/audit-refine/SKILL.md`](../.agents/skills/audit-refine/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/01-audit-entry-architecture.md`](../.kilo/commands/audit/phases/01-audit-entry-architecture.md) | Command | [`.agents/skills/01-entry-architecture/SKILL.md`](../.agents/skills/01-entry-architecture/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/02-audit-config-secrets.md`](../.kilo/commands/audit/phases/02-audit-config-secrets.md) | Command | [`.agents/skills/02-config-secrets/SKILL.md`](../.agents/skills/02-config-secrets/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/03-audit-async-concurrency.md`](../.kilo/commands/audit/phases/03-audit-async-concurrency.md) | Command | [`.agents/skills/03-async-concurrency/SKILL.md`](../.agents/skills/03-async-concurrency/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/04-audit-data-pipeline.md`](../.kilo/commands/audit/phases/04-audit-data-pipeline.md) | Command | [`.agents/skills/04-data-pipeline/SKILL.md`](../.agents/skills/04-data-pipeline/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/05-audit-config-templates.md`](../.kilo/commands/audit/phases/05-audit-config-templates.md) | Command | [`.agents/skills/05-config-templates/SKILL.md`](../.agents/skills/05-config-templates/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/09-audit-external-api.md`](../.kilo/commands/audit/phases/09-audit-external-api.md) | Command | [`.agents/skills/09-external-api/SKILL.md`](../.agents/skills/09-external-api/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/10-audit-code-quality.md`](../.kilo/commands/audit/phases/10-audit-code-quality.md) | Command | [`.agents/skills/10-code-quality/SKILL.md`](../.agents/skills/10-code-quality/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/11-audit-test-coverage.md`](../.kilo/commands/audit/phases/11-audit-test-coverage.md) | Command | [`.agents/skills/11-test-coverage/SKILL.md`](../.agents/skills/11-test-coverage/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/12-audit-production-ops.md`](../.kilo/commands/audit/phases/12-audit-production-ops.md) | Command | [`.agents/skills/12-production-ops/SKILL.md`](../.agents/skills/12-production-ops/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/13-audit-performance.md`](../.kilo/commands/audit/phases/13-audit-performance.md) | Command | [`.agents/skills/13-performance/SKILL.md`](../.agents/skills/13-performance/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/15-audit-authorization.md`](../.kilo/commands/audit/phases/15-audit-authorization.md) | Command | [`.agents/skills/15-authorization/SKILL.md`](../.agents/skills/15-authorization/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/16-audit-rendered-surface.md`](../.kilo/commands/audit/phases/16-audit-rendered-surface.md) | Command | [`.agents/skills/16-rendered-surface/SKILL.md`](../.agents/skills/16-rendered-surface/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/17-audit-arithmetic.md`](../.kilo/commands/audit/phases/17-audit-arithmetic.md) | Command | [`.agents/skills/17-arithmetic/SKILL.md`](../.agents/skills/17-arithmetic/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/phases/99-audit-validate.md`](../.kilo/commands/audit/phases/99-audit-validate.md) | Command | [`.agents/skills/99-validate/SKILL.md`](../.agents/skills/99-validate/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/tests/audit-bad-tests.md`](../.kilo/commands/audit/tests/audit-bad-tests.md) | Command | [`.agents/skills/audit-bad-tests/SKILL.md`](../.agents/skills/audit-bad-tests/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/tests/audit-test-performence.md`](../.kilo/commands/audit/tests/audit-test-performence.md) | Command | [`.agents/skills/audit-test-performance/SKILL.md`](../.agents/skills/audit-test-performance/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/tests/audit-tests-failures.md`](../.kilo/commands/audit/tests/audit-tests-failures.md) | Command | [`.agents/skills/audit-tests-failures/SKILL.md`](../.agents/skills/audit-tests-failures/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/audit/tests/audit-tests-full.md`](../.kilo/commands/audit/tests/audit-tests-full.md) | Command | [`.agents/skills/audit-tests-full/SKILL.md`](../.agents/skills/audit-tests-full/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/debug/bugfix.md`](../.kilo/commands/debug/bugfix.md) | Command | [`.agents/skills/bugfix/SKILL.md`](../.agents/skills/bugfix/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/docs/doc-architecture.md`](../.kilo/commands/docs/doc-architecture.md) | Command | [`.agents/skills/doc-architecture/SKILL.md`](../.agents/skills/doc-architecture/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/docs/doc-implementation-update.md`](../.kilo/commands/docs/doc-implementation-update.md) | Command | [`.agents/skills/doc-implementation-update/SKILL.md`](../.agents/skills/doc-implementation-update/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/docs/doc-readme.md`](../.kilo/commands/docs/doc-readme.md) | Command | [`.agents/skills/doc-readme/SKILL.md`](../.agents/skills/doc-readme/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/implement/implement-plan-multiagent.md`](../.kilo/commands/implement/implement-plan-multiagent.md) | Command | [`.agents/skills/implement-plan-multiagent/SKILL.md`](../.agents/skills/implement-plan-multiagent/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/commands/plan/spec-preparation.md`](../.kilo/commands/plan/spec-preparation.md) | Command | [`.agents/skills/spec-preparation/SKILL.md`](../.agents/skills/spec-preparation/SKILL.md) | One Skill; full adapted procedure in references/workflow.md; source declared name retained |
| [`.kilo/rules/commands.md`](../.kilo/rules/commands.md) | Toolchain/scheduling rules | [`AGENTS.md`](../AGENTS.md); [`.codex/config.toml`](../.codex/config.toml) | Commands, platform, coverage and scheduling preserved; verify against committed toolchain |
| [`.kilo/rules/project.md`](../.kilo/rules/project.md) | Project rules | [`AGENTS.md`](../AGENTS.md) | All 16 listed rules accounted for; duplicate standards consolidated |
| [`.kilo/skills/agent-md-refactor/LICENSE`](../.kilo/skills/agent-md-refactor/LICENSE) | Skill resource | [`.agents/skills/agent-md-refactor/LICENSE`](../.agents/skills/agent-md-refactor/LICENSE) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/agent-md-refactor/README.md`](../.kilo/skills/agent-md-refactor/README.md) | Skill resource | [`.agents/skills/agent-md-refactor/README.md`](../.agents/skills/agent-md-refactor/README.md) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/agent-md-refactor/SKILL.md`](../.kilo/skills/agent-md-refactor/SKILL.md) | Existing Skill | [`.agents/skills/agent-md-refactor/SKILL.md`](../.agents/skills/agent-md-refactor/SKILL.md) | Preserved procedure; unsupported metadata removed; invocation/scope adapted |
| [`.kilo/skills/artifacts-builder/LICENSE`](../.kilo/skills/artifacts-builder/LICENSE) | Skill resource | [`.agents/skills/artifacts-builder/LICENSE`](../.agents/skills/artifacts-builder/LICENSE) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/artifacts-builder/LICENSE.txt`](../.kilo/skills/artifacts-builder/LICENSE.txt) | Skill resource | [`.agents/skills/artifacts-builder/LICENSE.txt`](../.agents/skills/artifacts-builder/LICENSE.txt) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/artifacts-builder/SKILL.md`](../.kilo/skills/artifacts-builder/SKILL.md) | Existing Skill | [`.agents/skills/artifacts-builder/SKILL.md`](../.agents/skills/artifacts-builder/SKILL.md) | Preserved procedure; unsupported metadata removed; invocation/scope adapted |
| [`.kilo/skills/artifacts-builder/scripts/bundle-artifact.sh`](../.kilo/skills/artifacts-builder/scripts/bundle-artifact.sh) | Skill resource | [`.agents/skills/artifacts-builder/scripts/bundle-artifact.sh`](../.agents/skills/artifacts-builder/scripts/bundle-artifact.sh) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/artifacts-builder/scripts/init-artifact.sh`](../.kilo/skills/artifacts-builder/scripts/init-artifact.sh) | Skill resource | [`.agents/skills/artifacts-builder/scripts/init-artifact.sh`](../.agents/skills/artifacts-builder/scripts/init-artifact.sh) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/artifacts-builder/scripts/shadcn-components.tar.gz`](../.kilo/skills/artifacts-builder/scripts/shadcn-components.tar.gz) | Skill resource | [`.agents/skills/artifacts-builder/scripts/shadcn-components.tar.gz`](../.agents/skills/artifacts-builder/scripts/shadcn-components.tar.gz) | Byte-identical copy; licenses and bundled assets retained; scripts not executed |
| [`.kilo/skills/discuss-phase/SKILL.md`](../.kilo/skills/discuss-phase/SKILL.md) | Existing Skill | [`.agents/skills/discuss-phase/SKILL.md`](../.agents/skills/discuss-phase/SKILL.md) | Preserved procedure; unsupported metadata removed; invocation/scope adapted |
| [`.kilo/skills/fastapi-async-dev/SKILL.md`](../.kilo/skills/fastapi-async-dev/SKILL.md) | Existing Skill | [`.agents/skills/fastapi-async-dev/SKILL.md`](../.agents/skills/fastapi-async-dev/SKILL.md) | Preserved procedure; unsupported metadata removed; invocation/scope adapted |
| [`.kilo/skills/frontend-design/SKILL.md`](../.kilo/skills/frontend-design/SKILL.md) | Existing Skill | [`.agents/skills/frontend-design/SKILL.md`](../.agents/skills/frontend-design/SKILL.md) | Preserved procedure; unsupported metadata removed; invocation/scope adapted |
| [`.kilo/skills/readme-generator/SKILL.md`](../.kilo/skills/readme-generator/SKILL.md) | Existing Skill | [`.agents/skills/readme-generator/SKILL.md`](../.agents/skills/readme-generator/SKILL.md) | Preserved procedure; unsupported metadata removed; invocation/scope adapted |

## Referenced Dependencies and Cross-Links

| Dependency | State and Codex handling |
|---|---|
| `.ai/context/commands.md` | Absent in the source tree; command reads now use the root `AGENTS.md` verification/toolchain sections. No fictitious file was created. |
| `.ai/audit/templates/audit-findings.md` | Present and inspected; unchanged. The Codex workflows use the adapted `.codex/templates/audit-findings.md` so their severity and validation pointers resolve. |
| `.ai/tasks/templates/task_template.yaml` | Present and inspected; reused by plan implementation. Preserve semantic anchors, dependency metadata, source references and verbatim extra context. Example user-creation fields are examples, not this CLI's requirements. |
| `.ai/context/python-code-standards.md` | Present and inspected; already agrees with typing, Pydantic, enums, architecture and English rules. Original retained; no duplicate rule file created. |
| `.ai/context/test-command-patterns.md` | Present and inspected; use `AGENTS.md` and committed `pyproject.toml`/`Makefile.ps1` for executable commands. |
| `docs/00-overview/doc-maintenance-rules.md` | Present; read before report creation. Remains the documentation source of truth. |
| `ROADMAP.md`, `docs/SPEC.md`, `.ai/problems/CONTEXT_*.md` | Not present. `discuss-phase` requires a supplied existing phase scope or reports the missing prerequisite; it never invents it. |
| `.ai/problems/decisions/`, `.ai/problems/` | Output locations used by discuss/spec workflows; create when those workflows are actually invoked. No fake decision/specification was generated during migration. |
| `.ai/audit/99-validation/` | Existing findings inputs retained. New runs preserve IDs and completed artifacts; filenames ending `_done` remain original inputs, not proof of a new run. |
| Context7, context manager, external specialized roles | Context7 is optional if connected. No context-manager tool or additional named roles were configured by the source. Unsupported references are disclosed, with existing roles used only where their scope fits. |
| Existing artifact scripts and archive | Scripts and all licenses copied, not executed. The archive was inventoried (49 safe path members, 115785 uncompressed bytes) without extraction. No Node packages, frontend dependencies or global pnpm were installed. |

The key workflow graph is preserved: analyst → researcher → specification;
auditor → validator → validated findings; researcher → clarified recommendations;
planner → dependency-safe task → one implementor → docs-specialist → validator.
The main agent routes and checks completeness. It may assume the coordinator
role directly rather than add another orchestration level.

## Roles

| Role | Responsibility and completion | Restrictions |
|---|---|---|
| `analyst` | Business problem, owner decisions, conceptual tasks, complete specification and definition of ready | No implementation or architecture design; significant technical questions go to researcher |
| `auditor` | One assigned phase or explicitly assigned report synthesis; evidence, severity, type and report | No production edits, findings validation or implementation design; writes only assigned report |
| `docs-specialist` | Clear, consistent text, valid links/examples/frontmatter | Text files only; no source edits or MCP calls; large-context documentation workflow uses research summaries |
| `implementor` | Settled semantic task, scoped change, relevant lint/types/tests and authorized commit | One active implementor; no orchestration, unrelated cleanup, history rewrite or plans/phase edits prohibited by its workflow |
| `planner` | Atomic semantic tasks, dependency graph, rollout/risk gates, proportional verification | No production edits, architecture redefinition or line-based change instructions |
| `researcher` | Evidence, confidence, alternatives and preferred option when requested | No invented facts, ignored contradictions or production edits; technical docs verified against available sources |
| `tech-lead` | Precise delegation, owner/criteria, review and acceptance | Orchestrator only; no deep code investigation or implementation; never weakens original_prompt blocks |
| `test-engineer` | Environment/failure evidence, timings, root causes, taxonomy and execution strategy | Repository read-only, no delegation or Git writes; returns structured results to the report writer |
| `validator` | Evidence-based findings/plan/execution dispositions and readiness | No production edits; per-phase run writes only its report and does not repair source findings or shared template |

All roles inherit the parent's model, provider, approvals, sandbox and connected
servers. The complete source role prose is retained inside its TOML instructions,
with explicit invocation/output/completion boundaries. The source's universal
tool policy is preserved once in the permission matrix, including allow, ask and
deny patterns, rather than copied into each role.

## Skills and Invocation Examples

Use the following in a project chat, or pass the quoted prompt to `codex` from
this repository. These are **chat prompts**, not shell commands or Kilo slash
commands. Any paths/selections after the name are task context; no argument
parser or automatic `$ARGUMENTS` substitution is installed.

| Skill | Purpose | Example prompt |
|---|---|---|
| [`01-entry-architecture`](../.agents/skills/01-entry-architecture/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$01-entry-architecture Execute this phase only` |
| [`02-config-secrets`](../.agents/skills/02-config-secrets/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$02-config-secrets Execute this phase only` |
| [`03-async-concurrency`](../.agents/skills/03-async-concurrency/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$03-async-concurrency Execute this phase only` |
| [`04-data-pipeline`](../.agents/skills/04-data-pipeline/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$04-data-pipeline Execute this phase only` |
| [`05-config-templates`](../.agents/skills/05-config-templates/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$05-config-templates Execute this phase only` |
| [`09-external-api`](../.agents/skills/09-external-api/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$09-external-api Execute this phase only` |
| [`10-code-quality`](../.agents/skills/10-code-quality/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$10-code-quality Execute this phase only` |
| [`11-test-coverage`](../.agents/skills/11-test-coverage/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$11-test-coverage Execute this phase only` |
| [`12-production-ops`](../.agents/skills/12-production-ops/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$12-production-ops Execute this phase only` |
| [`13-performance`](../.agents/skills/13-performance/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$13-performance Execute this phase only` |
| [`15-authorization`](../.agents/skills/15-authorization/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$15-authorization Execute this phase only` |
| [`16-rendered-surface`](../.agents/skills/16-rendered-surface/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$16-rendered-surface Execute this phase only` |
| [`17-arithmetic`](../.agents/skills/17-arithmetic/SKILL.md) | Assigned content audit phase; complete independent blocks, evidence and severity rubric | `$17-arithmetic Execute this phase only` |
| [`99-validate`](../.agents/skills/99-validate/SKILL.md) | Validate supplied findings, retain IDs/dispositions and separate VAL findings | `$99-validate .ai/audit/09-external-api/findings.md` |
| [`agent-md-refactor`](../.agents/skills/agent-md-refactor/SKILL.md) | Progressive-disclosure instruction refactoring when separately requested | `$agent-md-refactor Review instruction organization` |
| [`artifacts-builder`](../.agents/skills/artifacts-builder/SKILL.md) | Complex standalone HTML artifacts with the existing bundled scripts | `$artifacts-builder <explicit separate artifact requirements and directory>` |
| [`audit-bad-tests`](../.agents/skills/audit-bad-tests/SKILL.md) | Environment gate, test blocks, independent analysis, validation, audit and remediation plan | `$audit-bad-tests Review tests/ without editing` |
| [`audit-multi-phase`](../.agents/skills/audit-multi-phase/SKILL.md) | Selected audit phases, output checks, conditional independent validation and bounded retries | `$audit-multi-phase 1,3,9` |
| [`audit-orphaned`](../.agents/skills/audit-orphaned/SKILL.md) | Reachability, references, history, protected categories and two report tables | `$audit-orphaned Report candidates without deleting files` |
| [`audit-phases-refinement`](../.agents/skills/audit-phases-refinement/SKILL.md) | Confirmed phases, sequential analysis/research/edit/review/adjustment | `$audit-phases-refinement 1,3` |
| [`audit-phases-review`](../.agents/skills/audit-phases-review/SKILL.md) | Coverage map, four independent research views, validator decision and conditional new phases | `$audit-phases-review Assess current coverage` |
| [`audit-refine`](../.agents/skills/audit-refine/SKILL.md) | Research non-actionable validated findings, one report at a time | `$audit-refine Process .ai/audit/99-validation/` |
| [`audit-test-performance`](../.agents/skills/audit-test-performance/SKILL.md) | Baseline, measurements, root causes, taxonomy, optimization research and plan | `$audit-test-performance Measure the current CI test path` |
| [`audit-tests-failures`](../.agents/skills/audit-tests-failures/SKILL.md) | Reproduce failures, investigate, classify and plan remediation | `$audit-tests-failures Analyze the latest failing suite` |
| [`audit-tests-full`](../.agents/skills/audit-tests-full/SKILL.md) | Whole-suite behavior/quality/coverage recommendations | `$audit-tests-full Audit tests/ without edits` |
| [`bugfix`](../.agents/skills/bugfix/SKILL.md) | Research, risk assessment, conditional approved plan, scoped fix and local quality checks | `$bugfix <existing bug description or file path>` |
| [`discuss-phase`](../.agents/skills/discuss-phase/SKILL.md) | Owner discussion with preserved question rounds and decision artifact | `$discuss-phase <existing phase scope or supplied scope text>` |
| [`doc-architecture`](../.agents/skills/doc-architecture/SKILL.md) | Full product/current-state/architecture investigation with owner decisions | `$doc-architecture Document the current CLI and library` |
| [`doc-implementation-update`](../.agents/skills/doc-implementation-update/SKILL.md) | Research significant features, verify implementation, assess docs and update | `$doc-implementation-update Use .ai/plans/07-verbosity-and-progress.md` |
| [`doc-readme`](../.agents/skills/doc-readme/SKILL.md) | Exact current setup, configuration, public interfaces and runnable README examples | `$doc-readme Update README.md from repository evidence` |
| [`fastapi-async-dev`](../.agents/skills/fastapi-async-dev/SKILL.md) | Conditional FastAPI guidance for an actual separately scoped API project | `$fastapi-async-dev <separate FastAPI task>` |
| [`frontend-design`](../.agents/skills/frontend-design/SKILL.md) | Conditional frontend design for a separately requested interface | `$frontend-design <separate interface task>` |
| [`implement-plan-multiagent`](../.agents/skills/implement-plan-multiagent/SKILL.md) | Inspect plan applicability, risk-aware blocks, serial implementation, docs and validation | `$implement-plan-multiagent .ai/plans/07-verbosity-and-progress.md` |
| [`readme-generator`](../.agents/skills/readme-generator/SKILL.md) | README-first repository evidence collection and writing | `$readme-generator Refresh README.md; do not commit or push` |
| [`spec-preparation`](../.agents/skills/spec-preparation/SKILL.md) | Resolve business uncertainty, research and write the next specification | `$spec-preparation <problem description or existing file path>` |

Explicit role handoff example:

```text
Use the auditor subagent for $09-external-api.
Read .agents/skills/09-external-api/SKILL.md and its required workflow.
Write .ai/audit/09-external-api/findings.md. Do not change application code.
Wait for completion and check the required output.
```

Complete scenario example:

```text
Use $audit-multi-phase for phases 1,3,9. Keep at most two subagents active,
preserve code, and run validator only where the Skill requires it.
```

To continue with research or implementation, invoke the relevant later Skill
explicitly with the actual generated report/specification/plan. An audit does
not implicitly authorize code changes or commits. Implementation Skills retain
their original per-item conventional-commit steps when invoked for authorized
implementation; the migration itself made no commits.

## Project Rule Coverage

No source rule is module-specific; all apply at the project level, so no nested
application `AGENTS.md` files were needed. The project rule file has 16 numbered
entries (the original numbering skips 13).

| Source project rule | Codex home and treatment |
|---|---|
| 1 English only | Root Coding Standards 1; documentation maintenance link |
| 2 Production code is king | Root Coding Standards 2; audit/test workflow role boundaries preserved |
| 3 Separation of concerns | Root Coding Standards 3 and existing architecture context |
| 4 Single responsibility | Root Coding Standards 4 |
| 5 Avoid overengineering | Root Coding Standards 5 |
| 6 Composition over inheritance | Root Coding Standards 6 |
| 7 Follow patterns; justify new abstractions | Root Coding Standards 5 and 7; no speculative design in planner/implementor |
| 8 Naming | Root Coding Standards 8 |
| 9 Pydantic v2 and strict typing; avoid any completely | Root Coding Standards 9 strengthened to prohibit new Any/untyped equivalents |
| 10 Enum/StrEnum fixed values, stored separately in models | Root Coding Standards 10; fixed vocabularies distinguished from ordinary data collections |
| 11 Pydantic v2 | Root Coding Standards 11 |
| 12 No print; module logging | Root Coding Standards 12; existing Rich CLI exception is preserved by layer |
| 14 Continuously current docs | Root Coding Standards 14 and documentation maintenance rules |
| 15 Small modules/functions | Root Coding Standards 13 |
| 16 Internationalization in DoD | Root Coding Standards 15; preserve English output, do not invent a new translation subsystem |
| 17 Workspace cleanliness | Root Coding Standards 16; unplaced scratch artifacts go only to .ai/tmp/ |

The commands rule file is covered as follows:

| Rule group | Codex home |
|---|---|
| Two concurrent subagents, one implementor, no fire-and-forget, resume sessions | Root Codex Roles and Workflows; native concurrency cap in config.toml |
| Windows/PowerShell/uv, no web/database/container prerequisites | Root context and Toolchain Details |
| Quick start and CLI entry points | Existing root Quick Reference plus Toolchain Details |
| Lint, fixes, formatting, mypy, dependencies and test commands | Root Quick Reference and Toolchain Details; source commands retained with actual tool distinctions |
| Ruff fix vs format behavior and Makefile wrapper | Toolchain Details; actual enabled Ruff rules remain controlled by pyproject.toml |
| Pytest options, coverage floor 70, slow selection, fixtures | Existing Test Environment plus Toolchain Details |
| CI runs lint then tests | Existing Quick Reference plus Toolchain Details |

## Adaptations, Conflicts and Limitations

| Source mechanism or conflict | Codex adaptation / remaining limit |
|---|---|
| Kilo Task(...), task mode, question, grep/glob/todo tools | Native delegation, available question/search/file/progress tools; no Task wrapper or plugin was invented. Required answers remain gates. |
| Kilo slash-command files and prompt placeholders | Explicit `$name` + supplied prompt context; bind workflow variables deliberately. Preserve actual selected phases, plan path and debug context. |
| audit-phase-gaps.md vs declared audit-phases-review | Preserve declared callable `audit-phases-review`; retain source filename in mapping. |
| audit-phase-refine.md vs declared audit-phases-refinement | Preserve declared callable `audit-phases-refinement`. |
| audit-test-performence.md typo vs declared audit-test-performance | Preserve declared callable `audit-test-performance`; source filename unchanged. |
| Phase NN-audit-name.md vs declared NN-name | Skills retain declared names such as `01-entry-architecture`; registry carries phase numbers, role and full procedure/rubric path. |
| Missing debug role | bugfix is coordinated by the main agent using the existing specialist sequence; no debug role created. |
| doc-specialist / Doc-specialist typo | Route to the existing `docs-specialist`. |
| Four independent researchers vs two-concurrent cap | Keep four reports, run two batches of two or serially with identical initial context. Do not leak earlier opinions into later independent tasks. |
| Nested coordinators may consume capacity | The root maintains the global two-active-agent and one-implementor budgets; when needed it brokers researcher calls for child coordinators. Per-session native caps alone are not a whole-tree guarantee. |
| Source no-background rule vs asynchronous native tools | Waiting/collection is mandatory; no unattended or fire-and-forget agents. Underlying native scheduling can still be asynchronous. |
| Per-pattern tool/edit permissions | Every source entry is in agent-permissions.md; scope is behavioral. Standalone custom agents inherit the parent's sandbox/approvals/MCP; there is no equivalent per-role Kilo ACL. No permission weakening was configured. |
| Conflicting broad wildcard allows vs role prose | Narrow role responsibilities and explicit workflow restrictions take precedence; no shell or alternate-tool bypass. Source pattern precedence is not portable; matrix interpretation is explicit. |
| Implementor read-only Git prose vs YAML add/commit allow | Read-only by default; only an applicable user-invoked implementation workflow authorizes its specific current-item new commit. Never rewrite history or include others' changes. |
| docs-specialist MCP deny | Retained as an instruction; inherited servers are not technically removed by a standalone role. Parent supplies necessary connector evidence. |
| mode, color, steps, options/id, alwaysApply, globs | Unsupported Kilo role/skill metadata is not serialized as invented Codex settings. Role name/scope and Skill trigger carry the applicable behavior; steps/color have no claimed exact equivalent. |
| readme-generator model: sonnet and tools list | Remove unsupported provider/model pin and Kilo tool labels; inherit the current model and tool permissions. Source README-first behavior is retained. |
| Validation writes -validated-findings.md but checks -validated.md | Use -validated-findings.md consistently, matching phase 99's output contract. |
| Audit retry rules disagree: one retry, two retries, second-failure escalation | Preserve absolute ceiling of three attempts; missing/empty-output gates use the stricter one retry and second-failure escalation. No silent third attempt. |
| Rejected findings cleaned before merge vs phase 99 cannot edit raw report | Exclude rejected findings from the actionable merged set; keep raw inputs and disposition history untouched. |
| Phase 99 claims seventeen content phases | Preserve as historical source text to verify; the actual catalog has 13 content phases. Never invent missing phases 06/07/08/14/18. |
| Template and phases may assert stale implementation details | Preserve audit questions and rubrics; assertions about eager bootstrap, YAML, exported data or retry mechanics are hypotheses to verify against current code, not new project truths. |
| Evidence wording varies (runtime vs static proof) | Keep phase-specific evidence criteria and disclose method/limits. Passing checks never become a finding or a validation verdict. |
| New-phase numbering has lowest-unused vs above-existing wording | Use the explicit step's lowest unused number, reserve 99; record the allocation and do not overwrite existing phases. |
| Phase refinement references Step 5 when its check is Step 2 | Final adjustments use Step 2's consistency report. Only selected Codex phase definitions are editable; Kilo definitions stay unchanged. |
| High-risk implement workflow selects validator but omits its pre-code slot | Explicitly place plan validation after planner and before implementor for high-risk blocks; fix/revalidate final findings before acceptance. |
| Bugfix duplicates pytest token | Use uv run pytest <relevant_path>; frontend commands are conditional on an actual separate frontend. |
| Implementor has unfinished '- If tests' fragment | Drop the empty fragment; retain the complete production-first test-quality principles around it. |
| docs discrepancy output has malformed backtick | Normalize the path to .ai/audit/problems/. |
| doc-architecture suggests uppercase root documents | Reuse existing lowercase docs/00-overview/ documents and their frontmatter, without duplicating content. |
| audit-tests-full objective says delete/rewrite; auditor is report-only | Produce tagged recommendations. Applying them requires an implementation task. |
| discuss-phase AskUserQuestion multi-select and source plan-phase/clear commands | Use an available question tool or chat; comma-separated selection if needed. Preserve four-question rounds, continuation/revisit/creation decisions and deferred ideas. Missing plan-phase is disclosed; direct planner handoff is only the closest available mechanism. |
| Generic supporting Skills mention extra services/frameworks | Retain them for separately requested matching work. FastAPI/frontend guidance must not trigger ordinary Python CLI changes or add server/database infrastructure. |
| artifacts-builder scripts install packages and delete generated outputs | Copied byte-for-byte, not executed. Optional future use requires scoped disposable output, inspected absolute delete targets and appropriate authorization; never run at this repository root. |
| agent-md-refactor recommends deleting generic rules/minimal root | Only applies if that refactor Skill is separately invoked; it did not override migration completeness or .kilo preservation. |

## Technical Verification

Checks run against the actual installed executable, not just an online example:

| Check | Result |
|---|---|
| Codex version/help and generated local app-server protocol schema | 0.163.0-alpha.10.2; available local read-only methods inspected |
| TOML parse | config.toml and all 9 standalone role files parsed with Python tomllib; each role has name, description and developer_instructions; no model/provider/permission override |
| Skill YAML/names | All 35 Skills passed the bundled skill-creator quick_validate.py logic; no scaffold placeholders; source licenses retained |
| Native strict config/read | Local app-server started with --strict-config; accepted project .codex layer, agents.enabled=true, max_concurrent_threads_per_session=2 |
| Native skills/list | All 35 migrated Skills found as enabled repo Skills; zero discovery errors |
| Native debug prompt-input | Completed without a model turn; rendered context includes root project rules and all 35 Skill names |
| Source coverage | All 54 Kilo files mapped; all 29 command procedures have their own linked reference; all role bodies and project rule groups reviewed |
| Procedure preservation | Detailed audit blocks, severity rubrics, report formats, owner decisions, conditional branches and original_prompt blocks retained; explicit corrections listed above |
| Relative documentation/reference links | Existing targets checked; fenced sample links and runtime placeholders treated as examples, not fabricated repository files |
| Source preservation | SHA-256 comparison: all 54 Kilo files identical; all 158 baseline-protected Kilo/application/test/build/CI files identical |
| Git diff scope and whitespace | Only AGENTS.md, .codex/, .agents/ and this migration report are deliverables; git diff --check run |

The initial sandboxed runtime probe could not access Codex's user-profile
SQLite state. It was rerun with approval and completed successfully. The
installed `debug` subcommand does not accept `--strict-config`, so strict parsing
was checked through app-server; debug prompt rendering was checked separately.
No user/global configuration was edited to make these checks pass.

### Explicitly Not Verified

- Actual custom-role spawning and application of its developer instructions in a
  fresh model session. TOML parses and the format matches official documentation,
  but prompt-input does not expose the role tool schema; merely seeing a role
  name in root instructions is not proof it was registered.
- End-to-end execution of any audit, specification, fix, implementation or
  independent-validation workflow; these would do real project work beyond migration.
- Model compliance with behavioral permissions or the global tree-wide scheduling
  budget. Native per-session concurrency was read back, but this is not a
  demonstration of a two-active-agent execution trace.
- External MCP tools/Context7, optional Bash/Node/pnpm workflows and frontend build
  scripts; no missing integrations or dependencies were installed.
- Application lint/type/test suite, Mediascope API access and product behavior.
  No application code changed; configuration validation was used instead of
  claiming unrelated application regression coverage.

Scratch inventory/probe/validation scripts and local protocol schemas are under
the already ignored `.ai/tmp/codex-migration/`; they are not runtime dependencies
or committed deliverables. Runtime probes only initialized a temporary local
server, read settings/Skills and rendered prompt context; no model turn ran.

## Use, Refresh and Maintain

1. Start a **new project chat** in Codex from this repository so the instruction
   chain and role catalog can be refreshed. Skills already passed local discovery.
   If a Skill is absent in an existing selector, restart the session/app.
2. Use `$skill-name` and supply existing input files or scope. A supplied phase
   selection is already a selection; do not ask the same question again. If a
   required decision is missing, stop only the dependent stage and state the gate.
3. For a role smoke test, ask: "Spawn the auditor role for a read-only check of
   the 09-external-api Skill; return its role and scope without running the audit
   or writing files." Confirm the runtime actually chose auditor and loaded its
   TOML instructions. This remains the manual acceptance check not run here.
4. To inspect instructions without a model turn, run `codex debug prompt-input`
   locally and inspect the project rules/Skill catalog. It can contain personal
   global instructions: do not publish its raw output. For live discovery, a
   local app-server's `skills/list` and `config/read` expose catalog and settings.
5. Add a role with a matching standalone `.codex/agents/name.toml` defining name,
   description and developer_instructions. Reuse the shared policy reference;
   do not add model/provider pins, sandbox overrides or legacy registrations.
6. Add a procedure as `.agents/skills/name/SKILL.md` with discriminating YAML
   name/description and detailed references/assets only where needed. Link every
   required resource and preserve real input, approval, retry and output gates.
7. Add universal rules to the root `AGENTS.md`. A genuinely module-scoped rule
   can use a nested `AGENTS.md`; process/role restrictions belong in their Skill
   or executor. Keep source correspondence in this report up to date.
8. For a new/refined audit phase, update the Codex Skill/reference and phase
   registry. Do not edit `.kilo/` automatically; parallel configuration maintenance
   is deliberate and requires its own scope. There is no bidirectional sync.

No Kilo cleanup, provider migration, dependency install, deployment or Git commit
is necessary to use the new configuration. Retain the Kilo originals.
