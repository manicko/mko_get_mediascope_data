---
id: settings-layers-architecture
domain: overview
tags:
  - architecture
  - configuration
  - settings
  - pydantic
  - json5
related:
  - doc-maintenance-rules
  - async-concurrency-architecture
  - product-overview
  - current-behavior
  - data-flow-architecture
  - system-architecture
  - open-questions
---

## Purpose

Single source of truth for how report settings layers combine in
`mko_get_mediascope_data`. Seller report definitions are authored as JSON5; the
tool loads them through a two-layer merge — a **DEFAULT** layer that carries the
full Mediascope API-parameter set plus per-subtype override blocks, and a
**CLIENT** layer that carries per-report deltas — then forwards the merged
parameters into the vendor `MediaVortexTask.build_task` call.

This document is the architecture reference behind the 2026-10-14 re-adjudication
of audit finding
[TPL-006](../../.ai/audit/99-validation/05-config-templates-validated-findings.md)
(shallow top-level overlay is intended, not a defect) and the re-grading of
[TPL-002](../../.ai/audit/99-validation/05-config-templates-validated-findings.md)
(pass-through consumption verified). Read this before modifying any
`settings/defaults/` or `settings/reports/` JSON5 or the merge logic in
`core/services.py`.

## Main Concepts

### The Two Settings Layers

The settings system has two layers, each loaded by a distinct Pydantic model:

| Aspect | DEFAULT layer | CLIENT layer |
|---|---|---|
| Path pattern | `settings/defaults/{media}/{report_type}.json5` | `settings/reports/**/*.json5` |
| Pydantic model | `DataDefaults` (`core/models.py:128`) | `ReportSettings` (`core/models.py:87`) |
| `model_config` | `ConfigDict(extra="allow")` (`core/models.py:129`) | `ConfigDict(extra="allow")` (`core/models.py:93`) |
| Declared field(s) | `DATA_DEFAULTS: dict` (`core/models.py:130`) | 11 declared fields (`core/models.py:94-110`) |
| Absorbed as extras | Subtype blocks (`DYNAMICS_BY_SPOTS`, `TOP_NAT_TV_PROGRAMS`, …) | API-parameter keys from templates (`slices`, `statistics`, `options`, …) |
| Role | Broad base: the full API-parameter set plus all per-subtype defaults | Delta: overrides or supplements keys for a specific report instance |

**Bundled DEFAULT files (all 6 combinations ship at HEAD, verified by `test_models.py::test_bundled_templates_validate`):**

| File | MediaType | ReportType | Key characteristics |
|---|---|---|---|
| `settings/defaults/tv/crosstab.json5` | TV | crosstab | 15 keys in `DATA_DEFAULTS`; subtype blocks `DYNAMICS_BY_SPOTS` (kitId=1, L59), `DYNAMICS_BY_SPOTS_DICT` (kitId=1, L87), `TOP_NAT_TV_ADVERTISERS` (no kitId — duplicate `issueType` at L107), `TOP_NAT_TV_PROGRAMS` (kitId=7, L128), `NAT_TV_CHANNELS_ATV` (kitId=1, L150), `NAT_TV_CHANNELS_BA` (kitId=7, L166), `NAT_TV_CHANNELS_SOC_DEM` (kitId=1, L181) |
| `settings/defaults/tv/timeband.json5` | TV | timeband | `DATA_DEFAULTS` only (includes `time_filter`), no subtype blocks |
| `settings/defaults/tv/simple.json5` | TV | simple | Ships at HEAD (was missing, now added) |
| `settings/defaults/tv_reg/crosstab.json5` | TV_REG | crosstab | 15 keys in `DATA_DEFAULTS`; `options.kitId=3` in base; subtype blocks `DYNAMICS_BY_SPOTS`, `DYNAMICS_BY_SPOTS_DICT` |
| `settings/defaults/tv_reg/timeband.json5` | TV_REG | timeband | Ships at HEAD (was missing, now added) |
| `settings/defaults/tv_reg/simple.json5` | TV_REG | simple | Ships at HEAD (was missing, now added) |

**CLIENT templates** live under `settings/reports/`. The reference (fully
validating) template is `settings/reports/nat_tv_brands_tefal.json5` — its two
entries are the only ones among all 32 bundled report entries that pass
`ReportSettings(**rep)` validation.

The DEFAULT and CLIENT layers are loaded at different points in the pipeline:

1. **CLIENT** — `AppService.get_report_settings` (`core/services.py:85-98`)
   iterates entries in a report JSON5 file and constructs
   `ReportSettings(**rep)` at `core/services.py:95`.
2. **DEFAULT** — `AppService.get_default_data_settings`
   (`core/services.py:100-114`) builds the path
    `defaults/{media.value.lower()}/{report_type.value.lower()}.json5` and
    constructs `DataDefaults(**utils.json5_to_dict(path))` at
   `core/services.py:114`.

### Merge Semantics

`AppService.merge_data_settings` (`core/services.py:116-133`) combines the two
layers. It is a **shallow top-level overlay**, not a deep merge. The steps are:

| Step | Code | What happens |
|---|---|---|
| 1 | `data_settings = defaults.DATA_DEFAULTS.copy()` (`services.py:120`) | Start with a shallow copy of the DEFAULT base dict (15 keys for crosstab). |
| 2 | `if subtype.value and hasattr(defaults, subtype.value):` + `data_settings.update(...)` (`services.py:122-124`) | If the report's `report_subtype` matches a subtype block absorbed as an extra on `DataDefaults`, overlay that block's keys via `dict.update` (whole-value replacement per key). |
| 3 | `for k, v in report_settings.model_dump().items(): if k in data_settings: data_settings[k] = v` (`services.py:126-128`) | Overlay top-level keys from the CLIENT layer, but **only** those that already exist in `data_settings`. Keys the client specifies replace the default value; keys the client does not specify keep their default. |
| 4 | `if report_settings.data_lang == "ru": data_settings["slices"] = utils.en_to_ru(...)` (`services.py:130-131`) | For Russian-language reports, translate slice names from English to Russian using `utils.en_to_ru` (`core/utils.py:223-233`). |

**Override rules (explicit):**

- **Whole-value replacement per top-level key.** `dict.update` and `data_settings[k] = v`
  each replace the entire value for a key — there is no recursive key-by-key merge.
- **A client key absent from the defaults is not added.** The `if k in data_settings`
  guard (`services.py:127`) ensures that CLIENT fields not present in `DATA_DEFAULTS`
  (e.g. `media`, `report_type`, `period`) never enter the merged data dict.
- **An omitted key keeps its default.** If the CLIENT layer does not specify a key
  that exists in `DATA_DEFAULTS`, the default value remains.

This shallow overlay is the intended contract. The CLIENT layer tunes the program
(it selects a subtype and overrides specific parameter keys); it does not replace
the entire configuration.

### Pass-Through Consumption

The merged dict is consumed as template-defined API parameters. `extra="allow"`
on `Data`, `DataDefaults`, and `ReportSettings` (`core/models.py:134/129/93`) is
the intended transport for these keys — they are absorbed as Pydantic extras and
forwarded into the API JSON:

```
merge_data_settings (dict)
  → Data(**data_settings)              (services.py:138)
  → report.data_settings.model_dump()   (reports.py:492 for DefaultTVTaskStrategy;
                                        reports.py:515 for TVRegTaskStrategy)
  → TVTask.settings                     (tasks.py:17)
  → send_task: build_task(report_type, **task.settings)
     (reports.py:350-352, calls
     MediaVortexTask.build_task)
  → vendor API JSON
```

Concretely:

1. `AppService.get_data_settings` (`core/services.py:135-138`) calls
   `merge_data_settings`, then wraps the result in `Data(**data_settings)`.
2. `TVMediaReport.__init__` reads `self.data_settings.options.get("kitId", 1)`
   (`core/reports.py:259-261`) — an example of consuming an extra key from the merged
   `Data` object before task generation.
3. The task strategy (`DefaultTVTaskStrategy.generate`, `core/reports.py:492`, or
    `TVRegTaskStrategy.generate`, `core/reports.py:515`) calls
   `report.data_settings.model_dump()` to dump all keys (the 3 declared
   `Data` fields: `options`, `statistics`, `slices` — plus 12 absorbed API-parameter
   keys).
4. Each `TVTask` stores the dump as `self.settings` (`core/tasks.py:17`).
5. `TVMediaReport.send_task` (`core/reports.py:344-361`) calls
    `self.builder` (which is `self.report_service.build_task`, set at
    `core/reports.py:111`) as
    `self.network_client.call(self.builder, task.report_type, **task.settings)`
    (`core/reports.py:350-352`).
6. The vendor `MediaVortexTask.build_task`
   reads each named parameter into the API JSON request body.

The `Data` model declares only 3 fields — `options`, `statistics`, `slices`
(`core/models.py:135-137`) — so the remaining 12 API-parameter keys
(`ad_filter`, `basedemo_filter`, `break_filter`, `company_filter`, `date_filter`,
`daytype_filter`, `frequency_dist_conditions`, `location_filter`, `program_filter`,
`sortings`, `targetdemo_filter`, `weekday_filter`) flow through as `extra="allow"`
extras.

### Template Conformance Rules

Architecture-first: when a template or default lags behind the architecture or
the Pydantic models, **repair the template, never the code**.

| Rule | Detail |
|---|---|
| Repairs are template-first | Stale templates are repaired to conform, never the code or models. |
| CLIENT carries deltas plus complete blocks | A client template provides only the deltas it intends to override; for any key it overrides, it must supply a complete block. |
| `options` blocks must be complete | Every `options` block declared in a template must carry `kitId` + `issueType` + `standRtg` where the default block carries them — no partial inheritance. |
| Parameter names match API names | Parameter names in templates must exactly match the names consumed by `build_task` (`statistics`, `ad_filter`, `slices`, `options`, `sortings`, `frequency_dist_conditions`, `*_filter`, …). |
| Reference template | `settings/reports/nat_tv_brands_tefal.json5` is the reference for a conformant CLIENT template — its two entries are the only bundled entries that pass `ReportSettings(**rep)` validation. |

## Common Misreadings

**(a) The shallow overlay is not a missing "deep merge" bug.** The shallow
top-level replacement in `merge_data_settings` (`core/services.py:120-128`) is the
intended contract: client values win where specified, defaults remain where the
client is silent. The `if k in data_settings` guard (`services.py:127`) means a
client key absent from `DATA_DEFAULTS` is not added — this prevents CLIENT-only
fields (`media`, `period`, …) from leaking into the API parameter dict.
Implementing a deep merge would break the two-layer overlay by partially merging
a subtype block into the base, changing semantics the vendor expects.

**(b) `extra="allow"` is not a silent absorber.** The keys absorbed as extras on
`Data`, `DataDefaults`, and `ReportSettings` are all forwarded into the API JSON
via `build_task(report_type, **task.settings)` (`core/reports.py:350-352`). A
typo'd key (e.g. `ad_filterr`) is silently dropped by the vendor's `build_task`
signature — this is a genuine residual risk (TPL-002 LOW), but it is mitigated by
template conformance: parameter names in templates must exactly match the names
consumed by `build_task`.

**(c) Do not "fix" these with `extra="forbid"`.** Narrowing `Data` or
`ReportSettings` to `extra="forbid"` would break the two-layer overlay, because
the DEFAULT layer's subtype blocks and the CLIENT layer's API-parameter keys are
absorbed precisely via `extra="allow"`. If stricter typing is desired, add the
known API parameters as declared fields on `Data` rather than forbidding extras.

## Related Docs

- [Doc Maintenance Rules](doc-maintenance-rules.md)
- [Async Concurrency Architecture](async-concurrency-architecture.md)
- [core/services.py](../../src/mko_get_mediascope_data/core/services.py)
- [core/models.py](../../src/mko_get_mediascope_data/core/models.py)
- [core/reports.py](../../src/mko_get_mediascope_data/core/reports.py)
- [core/tasks.py](../../src/mko_get_mediascope_data/core/tasks.py)
- [core/utils.py](../../src/mko_get_mediascope_data/core/utils.py)
- [core/paths.py](../../src/mko_get_mediascope_data/core/paths.py)
- [TPL-006 Re-validation Report](../../.ai/audit/99-validation/05-config-templates-validated-findings.md)
