---
id: doc-maintenance-rules
domain: overview
tags:
  - documentation
  - governance
  - rules
related:
  - current-behavior
  - async-concurrency-architecture
  - settings-layers-architecture
---

## Purpose

Mandatory reference for agents and guidance for humans when creating, updating, or splitting documentation. Read this before any task that modifies files under `docs/`.

## Main Concepts

- **Single source of truth:** Each fact lives in exactly one file.
- **Translate on touch:** All docs are English-only.
- **No silent dropping:** Unclear content goes to `docs/.tmp/`.
- **Frontmatter is contract:** Every domain doc starts with YAML frontmatter.

## Update Triggers

Update docs for:
- New features (new CLI commands, modules, enums)
- API changes (request/response format, status codes)
- Schema changes (new Pydantic model fields, enum values)
- Security changes (auth flow, credential handling, error exposure)

Do NOT update for:
- Minor refactoring
- Bug fixes
- Cosmetic changes
- Code comments

## File Placement Rules

- Numbered prefixes: `00-overview/`, `01-auth/`, `02-dashboards/`, etc.
- Flat structure preferred; max depth 2 levels.
- Group related docs by number prefix and domain folder.
- Use `98-reference/` for guides and external docs.

## Naming Conventions

- Use `kebab-case` for all filenames.
- Lowercase only; numbers for ordering.
- Specific names at deeper levels (e.g., `settings-layers-architecture.md`).
- Avoid generic names like `misc.md` or `notes.md`.

## Frontmatter Requirements

Every domain doc starts with YAML frontmatter:

```yaml
---
id: unique-identifier
domain: folder-name
tags:
  - relevant-tag-1
  - relevant-tag-2
related:
  - related-doc-id
---
```

Required fields: `id`, `domain`, `tags`, `related`.

## Content Quality Rules

- Every doc must have `## Purpose` section.
- Include `## Main Concepts` when applicable.
- Omit empty sections; mark TODOs in `docs/.tmp/`.
- Use tables for structured data.
- Link to relevant files using relative paths.

## Cross-Linking Strategy

- Reference SSOT (single source of truth) documents.
- Use relative paths: `[Text](../01-auth/auth-api.md)`.
- Include summary + cross-link rather than full copy.
- Verify all links are valid before commit.

## Doc Splitting Threshold

- **Soft threshold:** 800 lines — consider splitting.
- **Hard threshold:** 1000 lines — must split.
- Split at `##` boundaries into logical sections.
- Create new file in same domain folder.
- Update cross-links and related references.

## Translation Rules

- All documentation must be in English.
- No separate translation pass; translate during edits.
- Do not commit non-English content.
- Temporary uncertain content goes to `docs/.tmp/`.

## Output Guidelines

- Use `logger = logging.getLogger(__name__)` for all service layer output in `core/`
- Use `console.print()` (Rich) for CLI user-facing output in `app.py`
- Use `rich.progress.track()` for terminal progress bars — never use `print()` with `\r` manipulation
- Progress bars and user interaction belong to the CLI layer, not business logic

## Agent Enforcement

Agents must:
- Read this file before modifying any `docs/` content.
- Verify frontmatter on all new/modified docs.
- Check for English-only text.
- Ensure cross-links use relative paths.
- Validate line count against splitting threshold.

## Checklist Templates

### New Feature

- [ ] Created new doc with frontmatter
- [ ] Added to `## Related Docs` in parent docs
- [ ] Linked from relevant existing docs
- [ ] Purpose and Main Concepts sections included
- [ ] Cross-links use relative paths

### API Change

- [ ] Updated endpoint documentation
- [ ] Updated request/response examples
- [ ] Updated status codes if changed
- [ ] Updated cross-linked docs
- [ ] Verified all examples compile/run

### Schema Change (Pydantic model / Enum)

- [ ] Updated Pydantic model field definitions and descriptions in `core/models.py`
- [ ] Updated Enum member values in `core/models.py` (documented in architecture docs)
- [ ] Updated validation rules and error messages if constraints changed
- [ ] Verified examples still parse with updated schema
- [ ] Added migration notes for settings/config schema changes (W-01 template schema)
- [ ] If the settings/config file format changed (e.g. YAML → JSON5), updated the migration note in `README.md` and all architecture docs

### Doc Splitting

- [ ] Identified natural break at `##` boundary
- [ ] Created new file with proper frontmatter
- [ ] Moved content preserving structure
- [ ] Updated cross-links in both files
- [ ] Updated `related` arrays in frontmatter

### Code Quality Refactor (Enum extraction, shared-utility extraction, module split)

Used when a refactoring extracts a constant into an `Enum`, merges a duplicated
logic block into a shared utility, or splits an oversized module into a package:

- [ ] Added a `##` section (or updated an existing one) in the relevant architecture doc
      (e.g. `settings-layers-architecture.md` or `data-flow-architecture.md`) describing
      the new seam, utility, or split
- [ ] If a new `Enum` was introduced, documented the value-meaning table inline with
      cross-references (enums live in `core/models.py`)
- [ ] If a shared utility replaced duplicated logic, documented the old vs. new call paths
      and any intentionally-out-of-scope sites (e.g. direct vendor calls that bypass
      `NetworkClient`)
- [ ] If a module was split into a package, documented the sub-module layout, the shared
      object, and the re-export strategy in `__init__.py`
- [ ] Updated test import paths / patch targets in any test files that referenced the
      old location (verify with `grep`)
- [ ] Verified frontmatter is valid on all modified docs
- [ ] Checked for broken relative links in modified sections