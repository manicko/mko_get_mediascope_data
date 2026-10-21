# Audit Phase Registry

## Purpose

Codex routing and phase selection. Each content phase is independently callable; 99 validates output and is excluded from `all`. Missing numbers are deliberate gaps in the source, not new phases. Read the selected SKILL.md, then its complete workflow; its Severity Taxonomy is authoritative.

| Number | Skill | Executor | Workflow and severity rubric |
|---|---|---|---|
| 01 | [`01-entry-architecture`](../.agents/skills/01-entry-architecture/SKILL.md) | `auditor` | [Workflow](../.agents/skills/01-entry-architecture/references/workflow.md#severity-taxonomy) |
| 02 | [`02-config-secrets`](../.agents/skills/02-config-secrets/SKILL.md) | `auditor` | [Workflow](../.agents/skills/02-config-secrets/references/workflow.md#severity-taxonomy) |
| 03 | [`03-async-concurrency`](../.agents/skills/03-async-concurrency/SKILL.md) | `auditor` | [Workflow](../.agents/skills/03-async-concurrency/references/workflow.md#severity-taxonomy) |
| 04 | [`04-data-pipeline`](../.agents/skills/04-data-pipeline/SKILL.md) | `auditor` | [Workflow](../.agents/skills/04-data-pipeline/references/workflow.md#severity-taxonomy) |
| 05 | [`05-config-templates`](../.agents/skills/05-config-templates/SKILL.md) | `auditor` | [Workflow](../.agents/skills/05-config-templates/references/workflow.md#severity-taxonomy) |
| 09 | [`09-external-api`](../.agents/skills/09-external-api/SKILL.md) | `auditor` | [Workflow](../.agents/skills/09-external-api/references/workflow.md#severity-taxonomy) |
| 10 | [`10-code-quality`](../.agents/skills/10-code-quality/SKILL.md) | `auditor` | [Workflow](../.agents/skills/10-code-quality/references/workflow.md#severity-taxonomy) |
| 11 | [`11-test-coverage`](../.agents/skills/11-test-coverage/SKILL.md) | `auditor` | [Workflow](../.agents/skills/11-test-coverage/references/workflow.md#severity-taxonomy) |
| 12 | [`12-production-ops`](../.agents/skills/12-production-ops/SKILL.md) | `auditor` | [Workflow](../.agents/skills/12-production-ops/references/workflow.md#severity-taxonomy) |
| 13 | [`13-performance`](../.agents/skills/13-performance/SKILL.md) | `auditor` | [Workflow](../.agents/skills/13-performance/references/workflow.md#severity-taxonomy) |
| 15 | [`15-authorization`](../.agents/skills/15-authorization/SKILL.md) | `auditor` | [Workflow](../.agents/skills/15-authorization/references/workflow.md#severity-taxonomy) |
| 16 | [`16-rendered-surface`](../.agents/skills/16-rendered-surface/SKILL.md) | `auditor` | [Workflow](../.agents/skills/16-rendered-surface/references/workflow.md#severity-taxonomy) |
| 17 | [`17-arithmetic`](../.agents/skills/17-arithmetic/SKILL.md) | `auditor` | [Workflow](../.agents/skills/17-arithmetic/references/workflow.md#severity-taxonomy) |
| 99 | [`99-validate`](../.agents/skills/99-validate/SKILL.md) | `validator` | [Workflow](../.agents/skills/99-validate/references/workflow.md#severity-taxonomy) |

New phase: use the lowest unused two-digit number except 99, create a new Skill and workflow following the existing phase structure, and register it here. Keep the original Kilo phases intact.
