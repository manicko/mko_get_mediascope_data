# Mediascope TVI API Parameter Reference

> **Last verified:** 2026-10-28 — sources: [url_1] statistics, [url_2] Timeband filters/slices, [url_2b] Simple filters/slices, [url_3] `_help.ipynb` notebook, vendor library `mediascope-api-lib` 1.8.3, project templates and report files.

This document is a static parameter reference for the Mediascope TV Index (TVI) API — the backend that the `mko_get_mediascope_data` tool queries to fetch TV audience data. Each TVI API "report type" corresponds to a data query submitted via `MediaVortexTask.build_<type>_task(...)` and polled through the task lifecycle (`build_task` → `send_task` → `wait_task` → `get_result` → `result2table`).

The reference covers all six vendor report types:

| Report Type | Supported by `mko_get_mediascope_data`? |
|---|---|
| `timeband` | ✅ Yes |
| `simple` | ✅ Yes |
| `crosstab` | ✅ Yes |
| `consumption-target` | ❌ No (vendor-only) |
| `duplication-timeband` | ❌ No (vendor-only) |
| `respondent-analysis` | ⚠️ Undocumented (vendor-only) |

Parameters are split into two groups:

- **Common Parameters** — accepted by all documented report types; documented once here.
- **Per-Report-Type sections** — parameters specific to individual report types.

> **Last verified: 2026-10-28 — sources: [url_1], [url_2], [url_2b], [url_3], vendor_library v1.8.3**

---

## Common Parameters (All Report Types)

Parameters accepted by every documented TVI API report type (`timeband`, `simple`, `crosstab`, `consumption-target`, `duplication-timeband`). Each is documented once here with full detail. Per-report-type sections list these with a `[COMMON]` badge linking back to `#common-parameters`.

> ⚠️ **Note on `respondent-analysis`:** This 6th report type is entirely undocumented in the vendor docs and notebook (see [§21 Content Gaps](#21-content-gaps--undocumented-parameters)). The "common" classification below is based on the 5 documented report types. `respondent-analysis` is assumed to accept the same common parameters by inheritance, but this is unverified.

### Common Filters (8)

| # | Parameter | JSON Key | Type | Default | Supported By | Description | Templates |
|---|---|---|---|---|---|---|---|
| 1 | `date_filter` | `filter.dateFilter` | `list[tuple[str, str]]` | `null` | All (5 documented types) | Filter by date range as `(start, end)` tuples in `YYYY-MM-DD`. Multiple periods allowed. Required (`*` in vendor docs). | `[TV]`, `[TV-Reg]` DATA_DEFAULTS; set dynamically by `CODE` |
| 2 | `weekday_filter` | `filter.weekDayFilter` | `str` | `null` | All (5 documented types) | Filter by research weekday expression over `researchWeekDay`. | All DATA_DEFAULTS |
| 3 | `daytype_filter` | `filter.dayTypeFilter` | `str` | `null` | All (5 documented types) | Filter by day type expression over `researchDayType`. | All DATA_DEFAULTS |
| 4 | `company_filter` | `filter.companyFilter` | `str` | `null` | All (5 documented types) | Filter by channel/company/thematic/net/region/holding IDs. For TV_REG, set programmatically at `reports.py:829` in `TVRegTaskStrategy.generate`. | All DATA_DEFAULTS; `CODE` (reports.py:829); `[RF: nat_tv_tvreport.json5]` (6 of 7 entries) |
| 5 | `region_filter` | `filter.regionFilter` | `str` | `null` | All (5 documented types) ⚠️ **DEPRECATED** | Filter by region. **Deprecated** — vendor prints a runtime notice and recommends using `company_filter` with `regionId` instead. Correctly omitted from all templates. | None — correctly omitted |
| 6 | `location_filter` | `filter.locationFilter` | `str` | `null` | All (5 documented types) | Filter by viewing location expression over `locationId`. | All DATA_DEFAULTS |
| 7 | `basedemo_filter` | `filter.baseDemoFilter` | `str` | `null` | All (5 documented types) | Base demographic filter expression over demo attributes (`sex`, `age`, `incomeGroup`, etc.). Set dynamically from `target_audiences` via `reports.py:787` (DefaultTVTaskStrategy) and `reports.py:833` (TVRegTaskStrategy). | All DATA_DEFAULTS; `CODE` (reports.py:787, 833); `[RF: nat_tv_tvreport.json5]` |
| 8 | `targetdemo_filter` | `filter.targetDemoFilter` | `str` | `null` | All (5 documented types) | Additional target audience filter for target-vs-base ratio (e.g., Affinity Index). | All DATA_DEFAULTS; `[RF: nat_tv_tvreport.json5]` (`'sex = 2'` in 1 entry) |

### Common Non-Filter Parameters

| # | Parameter | JSON Key | Type | Default | Supported By | Description | Templates |
|---|---|---|---|---|---|---|---|
| 9 | `slices` | `slices` | `list[str]` | `[]` / `null` | All (5 documented types) | List of slice dimensions for grouping/segmentation. Empty list or null means no slicing. | TV simple/timeband: `[]`; TV crosstab: `null`; TV_REG all: `[]`; TV_REG crosstab: `['regionName']` |
| 10 | `statistics` | `statistics` | `list[str]` | `null` | All (5 documented types) | List of metric names to calculate. Must be non-empty (vendor `check_task` validates). | All DATA_DEFAULTS (`null`); subtypes and report files override |
| 11 | `options` | `options` | `dict` | `{}` / `null` | All (5 documented types) | Per-task calculation options (kitId, issueType, standRtg, totalType, etc.). Full subkey documentation in [§7 Options Dictionary](#7-options-dictionary). | TV: `null` in DATA_DEFAULTS; TV_REG: full dict; subtypes and report files override |
| 12 | `scales` | `scales` | `dict` | `null` | All (5 documented types) ⚠️ **Conditional** | Scale buckets for `drfd`/`reachN` statistics. Format: `{'<stat>': [(from, to), ...]}`. Required when statistics include `drfd` or `reachN`. Undocumented in vendor docs — only evidence is `checks._check_scales`. | None |
| 13 | `reach_conditions` | `reachConditions` | `dict` | `null` | All (5 documented types) | Reach threshold conditions (`filterType`, `viewingType`, `intervals`). Full docs in [§11 Reach Conditions](#11-reach-conditions). | None |
| 14 | `task_name` | (auto-generated) | `str` | Auto | All (5 documented types) | Task name string. Defaults to `user + type + datetime`. Generated by code at `reports.py:785-796` (DefaultTVTaskStrategy) and `reports.py:834-844` (TVRegTaskStrategy). | None — auto-generated by `CODE` |
| 15 | `sortings` | `sorting` | `dict` | `null` | `timeband`, `simple`, `crosstab` | Dict `{column: 'ASC'\|'DESC'}`; library wraps into `{sorting: {sortingUnits: [{unit, direction}]}}`. Full docs in [§10 Sortings](#10-sortings). | All DATA_DEFAULTS (`null`) |
| 16 | `frequency_dist_conditions` | `frequencyDistConditions` | `dict` | `null` | `simple`, `crosstab` | Frequency distribution intervals for `FrequencyDist*` statistics. Full docs in [§12 Frequency Distribution Conditions](#12-frequency-distribution-conditions). | TV crosstab DATA_DEFAULTS; TV_REG all DATA_DEFAULTS; `[RF: nat_tv_brands_weekly_reach.json5]` |
| 17 | `consumption_target_options` | `consumptionTargetOptions` | `dict` | `null` | `consumption-target` only | Options for consumption-target analysis. Full docs in [§16 Consumption Target Options](#16-consumption-target-options). | None |
| 18 | `add_city_to_basedemo_from_region` | `options/add_city_to_basedemo_from_region` | `bool` | `true` (TV_REG) | TV_REG only (kitId=3) | Client-side: auto-populates `basedemo_filter` with city derived from company region. See [§15 City Dataset Features](#15-city-dataset-features-kitid3). | TV_REG all DATA_DEFAULTS (`true`) |
| 19 | `add_city_to_targetdemo_from_region` | `options/add_city_to_targetdemo_from_region` | `bool` | `true` (TV_REG) | TV_REG only (kitId=3) | Client-side: auto-populates `targetdemo_filter` with city derived from company region. See [§15 City Dataset Features](#15-city-dataset-features-kitid3). | TV_REG all DATA_DEFAULTS (`true`) |

> ⚠️ **Common but conditional:** `scales`, `frequency_dist_conditions`, `consumption_target_options`, `add_city_to_*` are present in the common parameter list but are type-scoped. See per-report-type sections and the [Filter Support Matrix](#3-filter-support-matrix) for details.

> **Last verified: 2026-10-28 — sources: [url_1], [url_2], [url_2b], [url_3], vendor_library v1.8.3**

---

## Filter Support Matrix

Compact reference: which report types support each **non-common** filter. Common filters (the 8 in [§2](#2-common-parameters-all-report-types)) are excluded — they are universal. `respondent-analysis` is excluded as entirely undocumented.

| Filter | timeband | simple | crosstab | consumption-target | duplication-timeband |
|--------|:--------:|:------:|:--------:|:------------------:|:--------------------:|
| `time_filter` | ✅ | ❌ | ❌ | ✅ | ✅ |
| `respondent_filter` | ✅ | ✅ | ✅ | ❌ | ❌ |
| `platform_filter` | ✅ | ✅ | ✅ | ❌ | ❌ |
| `playbacktype_filter` | ✅ | ✅ | ✅ | ❌ | ❌ |
| `bigtv_filter` | ✅ | ✅ | ✅ | ❌ | ❌ |
| `program_filter` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `break_filter` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `ad_filter` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `subject_filter` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `duration_filter` | ❌ | ❌ | ❌ | ✅ | ✅ |
| `duplication_company_filter` | ❌ | ❌ | ❌ | ❌ | ✅ |
| `duplication_time_filter` | ❌ | ❌ | ❌ | ❌ | ✅ |
| `frequency_dist_conditions` | ❌ | ✅ | ✅ | — | — |
| `sortings` | ✅ | ✅ | ✅ | ❌ | ❌ |
| `consumption_target_options` | — | — | — | ✅ | — |
| `custom_demo_variable_id` | — | — | — | ✅ | — |
| `custom_company_variable_id` | — | — | — | ✅ | — |
| `custom_respondent_variable_id` | — | — | — | ✅ | — |
| `custom_time_variable_id` | — | ❌ | ✅ | — | — |
| `custom_duplication_time_variable_id` | — | — | — | — | ✅ |
| `custom_duplication_company_variable_id` | — | — | — | — | ✅ |

> ✅ = supported; ❌ = not supported; — = not applicable (no vendor support / undocumented); ⚠️ = deprecated; 📋 = project-level.

> `frequency_dist_conditions` IS supported by `simple` per the vendor `filter_support_matrix_by_report` (`["simple", "crosstab"]`). Only the `timeband` report type does not support it.

> **Last verified: 2026-10-28 — sources: [url_1], [url_2], [url_2b], [url_3], vendor_library v1.8.3**

---

## Per-Report-Type Sections

### 4.1 Timeband

> **Metadata:** `api_task_type` = `timeband` · `api_endpoint` = `/task/timeband` · `user_face_name` = "Отчет Периоды (Timeband)" · `library_builder` = `build_timeband_task` / `cats.get_timeband_unit(kit_id)` · `applies_to` = TV, TV_REG · `options_defaults` = `{kitId: 1, totalType: "TotalChannels", minSmp: 5, useNbd: true, standRtg: {useRealDuration: false, standardDuration: 5}, viewingSubject: "RESPONDENT"}` · `sortings_supported` = ✅

- **Common filters:** [COMMON](#common-parameters-all-report-types) — `date_filter`, `weekday_filter`, `daytype_filter`, `company_filter`, `region_filter` ⚠️, `location_filter`, `basedemo_filter`, `targetdemo_filter`
- **Time-specific:** `time_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Response filters:** `respondent_filter`, `platform_filter`, `playbacktype_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Sortings:** ✅ supported
- **Frequency distribution:** ❌ not supported
- **Scales:** ✅ conditional (requires `drfd`/`reachN` statistics)

#### Statistics (29)

| Statistic | Unit | Scope | Description (EN) |
|---|---|---|---|
| `Reach000` | thousand persons | period | Accumulated reach in thousands. Watched ≥1 min during the period. |
| `ReachPer` | % | period | Accumulated reach as % of total audience. |
| `AvWkReach000` | thousand persons | week | Average weekly reach in thousands (≥1 min/week). |
| `AvWkReachPer` | % | week | Average weekly reach in %. |
| `AvMReach000` | thousand persons | month | Average monthly reach in thousands. |
| `AvMReachPer` | % | month | Average monthly reach in %. |
| `AvReach000` | thousand persons | day | Average daily reach in thousands (≥1 min/day). |
| `AvReachPer` | % | day | Average daily reach in %. |
| `Rtg000` | thousand persons | — | Average target-audience viewers across the whole interval. |
| `RtgPer` | % | — | Rating in % of total audience size. |
| `TgAffinPer` | index | — | Affinity index = target Rtg% / base Rtg% of the same interval. |
| `TgSatPer` | index | — | Demo profile = avg target Rtg(000) / avg base Rtg(000) of the same interval. |
| `TgAffinReachPer` | index | — | Affinity index based on reach (target reach% / base reach%). |
| `TgSatReachPer` | index | — | Demo profile based on reach (target reach(000) / base reach(000)). |
| `TAud` | minutes | — | Total viewing time across all viewers over the period. |
| `ATV` | minutes | — | Average daily viewing time among the population. |
| `ATVReach` | minutes | — | Average daily viewing time among viewers. |
| `Share` | % | interval | Share of audience watching the interval. |
| `WAgeAverage` | years | — | Weighted average audience age. |
| `Universe000` | thousand persons | — | Total population size of the target audience. |
| `Sample` | respondents | — | Sample size (respondents). |
| `TTVRtg000` | thousand persons | — | Total TV rating in thousands (vs Total channels). |
| `TTVRtgPer` | % | — | Total TV rating in %. |
| `TTVReach000` | thousand persons | — | Total TV accumulated reach in thousands. |
| `TTVReachPer` | % | — | Total TV accumulated reach in %. |
| `TTVAvReach000` | thousand persons | — | Total TV average daily reach in thousands. |
| `TTVAvReachPer` | % | — | Total TV average daily reach in %. |
| `TTVATV` | minutes | — | Total TV average daily viewing time among the population. |
| `TTVATVReach` | minutes | — | Total TV average daily viewing time among viewers. |

#### Slices

Timeband slices include all [§6 Slices Dictionary](#6-slices-dictionary) entries plus the timeband-only `timeBand1`, `timeBand5`, `timeBand10`, `timeBand15`, `timeBand30`, `timeBand60` slices.

> **Last verified: 2026-10-28 — sources: [url_1], [url_2]**

### 4.2 Simple

> **Metadata:** `api_task_type` = `simple` · `api_endpoint` = `/task/simple` · `user_face_name` = "Отчет События (Simple)" · `library_builder` = `build_simple_task` / `cats.get_simple_unit(kit_id)` · `applies_to` = TV, TV_REG · `options_defaults` = `{kitId: 1, issueType: "PROGRAM", standRtg: {useRealDuration: false, standardDuration: 5}, viewingSubject: "RESPONDENT", totalType: "TotalChannels", minSmp: 5}` · `sortings_supported` = ✅ · `frequency_dist_conditions_supported` = ✅

- **Common filters:** [COMMON](#common-parameters-all-report-types) — `date_filter`, `weekday_filter`, `daytype_filter`, `company_filter`, `region_filter` ⚠️, `location_filter`, `basedemo_filter`, `targetdemo_filter`
- **Program/event filters:** `program_filter`, `break_filter`, `ad_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Response filters:** `respondent_filter`, `platform_filter`, `playbacktype_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Duration filter:** ❌ not supported
- **Sortings:** ✅ supported
- **Frequency distribution:** ✅ supported
- **Custom variables:** `custom_time_variable_id` ❌ (Crosstab-only)

#### Statistics (53)

| Statistic | Unit | Scope | Description (EN) |
|---|---|---|---|
| `Reach000` | thousand persons | event | Reach (contact ≥1 min with the event). Also in Timeband. |
| `SpotByBreaksReach000` | thousand persons | — | Reach in thousands, by ad-block significant time. |
| `SpotByProgramReach000` | thousand persons | — | Reach in thousands, by program significant time. |
| `ReachPer` | % | event | Reach in %. |
| `SpotByBreaksReachPer` | % | — | Reach in %, by ad-block significant time. |
| `SpotByProgramReachPer` | % | — | Reach in %, by program significant time. |
| `Rtg000` | thousand persons | — | Average target-audience viewers across full event duration. |
| `SalesRtg000` | thousand persons | — | Average viewers in the channel's billing audience. |
| `SpotByBreaksRtg000` | thousand persons | — | Rating(000) by ad-block significant time. |
| `SpotByBreaksSalesRtg000` | thousand persons | — | SalesRtg(000) by ad-block significant time. |
| `SpotByProgramRtg000` | thousand persons | — | Rating(000) by program significant time. |
| `RtgPer` | % | — | Rating in %. |
| `StandRtgPer` | % | — | Standard rating % = Rtg% × expectedDuration / 20. |
| `SalesRtgPer` | % | — | Rating % by channel billing audience. |
| `StandSalesRtgPer` | % | — | Standard sales rating % = SalesRtg% × expectedDuration / 20. |
| `SpotByBreaksRtgPer` | % | — | Rating % by ad-block significant time. |
| `SpotByBreaksStandRtgPer` | % | — | Standard rating % by ad-block significant time. |
| `SpotByBreaksSalesRtgPer` | % | — | Sales rating % by ad-block significant time. |
| `SpotByBreaksStandSalesRtgPer` | % | — | Standard sales rating % by ad-block significant time. |
| `SpotByProgramRtgPer` | % | — | Rating % by program significant time. |
| `Share` | % | — | Audience share (viewers / total target-group TV viewers during the event). |
| `AccumulativeReach000` | thousand persons | — | Cumulative reach across events (running total). |
| `SpotByBreaksAccumulativeReach000` | thousand persons | — | Cumulative reach by ad-block significant time. |
| `AccumulativeReachPer` | % | — | Cumulative reach in % of target audience. |
| `SpotByBreaksAccumulativeReachPer` | % | — | Cumulative reach in % by ad-block significant time. |
| `Frequency` | contacts | — | Average number of contacts per viewer with the events. |
| `SpotByBreaksFrequency` | contacts | — | Frequency by ad-block significant time. |
| `FrequencyDist000` | thousand persons | — | Frequency distribution of reach in thousands. Configured via `frequency_dist_conditions`. |
| `SpotByBreaksFrequencyDist000` | thousand persons | — | Frequency distribution of reach (by ad-block significant time). |
| `FrequencyDistPer` | % | — | Frequency distribution of reach in %. |
| `SpotByBreaksFrequencyDistPer` | % | — | Frequency distribution of reach in % (by ad-block significant time). |
| `TgAffinPer` | index | — | Affinity index = event Rtg(target) / event Rtg(base). |
| `TgAffinReachPer` | index | — | Affinity index based on reach. |
| `SalesAffinPer` | index | — | Affinity index vs channel billing audience. |
| `TgSatPer` | index | — | Demo profile = avg target Rtg(000) / avg base Rtg(000). |
| `TgSatReachPer` | index | — | Demo profile by reach. |
| `SpotByBreaksTgSatReachPer` | index | — | Demo profile by reach, ad-block significant time. |
| `TAud` | minutes | — | Total viewing time for all viewers for one event. |
| `ATV` | minutes | — | Average viewing time among the population for one event. |
| `ATVReach` | minutes | — | Average viewing time among viewers for one event. |
| `Fidelity` | % | — | Average viewing time / event duration. |
| `Universe000` | thousand persons | — | Total population size in thousands. |
| `Sample` | respondents | — | Sample size (respondents in target group). |
| `TTVRtg000` | thousand persons | — | Total TV rating in thousands (vs Total channels). |
| `TTVRtgPer` | % | — | Total TV rating in %. |
| `Duration` | seconds | — | Actual (unrounded) event duration in seconds. |
| `Quantity` | events | — | Number of broadcast events. |
| `CostByMinRUB` | RUB | — | Placement cost from ad protocol per minute pricelists (RUB). |
| `CostByMinUSD` | USD | — | Placement cost per minute pricelists (USD). |
| `CostByGRPRUB` | RUB | — | Placement cost per GRP pricelists (RUB). |
| `CostByGRPUSD` | USD | — | Placement cost per GRP pricelists (USD). |
| `ConsolidatedCostRUB` | RUB | — | Consolidated placement cost by ad-block assessment type (RUB). |
| `ConsolidatedCostUSD` | USD | — | Consolidated placement cost by ad-block assessment type (USD). |

#### Slices

Simple slices mirror Timeband plus the full program/break/ad/platform/playback slice families (see [§6](#6-slices-dictionary)).

> **Last verified: 2026-10-28 — sources: [url_1], [url_2b]**

### 4.3 Crosstab

> **Metadata:** `api_task_type` = `crosstab` · `api_endpoint` = `/task/crosstab` · `user_face_name` = "Отчет Кросс-таблица (Crosstab)" · `library_builder` = `build_crosstab_task` / `cats.get_crosstab_unit(kit_id)` · `applies_to` = TV, TV_REG · `options_defaults` = `{kitId: 1, issueType: "PROGRAM", standRtg: {useRealDuration: false, standardDuration: 5}, viewingSubject: "RESPONDENT", totalType: "TotalChannels", minSmp: 5}` · `sortings_supported` = ✅ · `frequency_dist_conditions_supported` = ✅

- **Common filters:** [COMMON](#common-parameters-all-report-types)
- **Program/event filters:** `program_filter`, `break_filter`, `ad_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Response filters:** `respondent_filter`, `platform_filter`, `playbacktype_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Time filter:** ❌ not supported
- **Sortings:** ✅ supported
- **Frequency distribution:** ✅ supported
- **Custom variables:** `custom_time_variable_id` ✅ ([Matrix](#3-filter-support-matrix))
- **Crosstab-specific option:** `issueType` selects the event aggregation grain (PROGRAM / BREAKS / AD)

#### Statistics (74 — 39 Sum + 35 Avg)

**Sum (total) statistics (39):**

| Statistic | Unit | Description (EN) |
|---|---|---|
| `CumReach000` | thousand persons | Cumulative reach in thousands. |
| `SpotByBreaksCumReach000` | thousand persons | Cumulative reach by ad-block significant time. |
| `SpotByProgramCumReach000` | thousand persons | Cumulative reach by program significant time. |
| `CumReachPer` | % | Cumulative reach in %. |
| `SpotByBreaksCumReachPer` | % | Cumulative reach in % by ad-block significant time. |
| `SpotByProgramCumReachPer` | % | Cumulative reach in % by program significant time. |
| `OTS000Sum` | thousand persons | Opportunity to See in thousands (Sum). |
| `OTSPerSum` | % | Opportunity to See in % (Sum). |
| `SpotByBreaksOTS000Sum` | thousand persons | OTS in thousands by ad-block significant time (Sum). |
| `SpotByBreaksOTSPerSum` | % | OTS in % by ad-block significant time (Sum). |
| `Rtg000Sum` | thousand persons | Rating in thousands (Sum). |
| `SalesRtg000Sum` | thousand persons | Billing rating in thousands (Sum). |
| `SpotByBreaksRtg000Sum` | thousand persons | Rating in thousands by ad-block significant time (Sum). |
| `SpotByBreaksSalesRtg000Sum` | thousand persons | Billing rating in thousands by ad-block significant time (Sum). |
| `SpotByProgramRtg000Sum` | thousand persons | Rating in thousands by program significant time (Sum). |
| `RtgPerSum` | % | Rating in % (Sum). |
| `StandRtgPerSum` | % | Standard rating % (Sum). |
| `SalesRtgPerSum` | % | Sales rating % (Sum). |
| `StandSalesRtgPerSum` | % | Standard sales rating % (Sum). |
| `SpotByBreaksRtgPerSum` | % | Rating % by ad-block significant time (Sum). |
| `SpotByBreaksStandRtgPerSum` | % | Standard rating % by ad-block significant time (Sum). |
| `SpotByBreaksSalesRtgPerSum` | % | Sales rating % by ad-block significant time (Sum). |
| `SpotByBreaksStandSalesRtgPerSum` | % | Standard sales rating % by ad-block significant time (Sum). |
| `SpotByProgramRtgPerSum` | % | Rating % by program significant time (Sum). |
| `FrequencyDist000` | thousand persons | Frequency distribution of reach in thousands (Sum). |
| `SpotByBreaksFrequencyDist000` | thousand persons | Frequency distribution by ad-block (Sum). |
| `FrequencyDistPer` | % | Frequency distribution of reach in % (Sum). |
| `SpotByBreaksFrequencyDistPer` | % | Frequency distribution by ad-block (Sum). |
| `TAudSum` | minutes | Total viewing time (Sum). |
| `ATVSum` | minutes | Average daily viewing time among population (Sum). |
| `ATVReachSum` | minutes | Average daily viewing time among viewers (Sum). |
| `DurationSum` | seconds | Total event duration (Sum). |
| `QuantitySum` | events | Number of events (Sum). |
| `CostByMinSumRUB` | RUB | Placement cost per minute (Sum). |
| `CostByMinSumUSD` | USD | Placement cost per minute (Sum). |
| `CostByGRPSumRUB` | RUB | Placement cost per GRP (Sum). |
| `CostByGRPSumUSD` | USD | Placement cost per GRP (Sum). |
| `ConsolidatedCostSumRUB` | RUB | Consolidated placement cost (Sum). |
| `ConsolidatedCostSumUSD` | USD | Consolidated placement cost (Sum). |

**Average statistics (35):**

| Statistic | Unit | Description (EN) |
|---|---|---|
| `AvReach000` | thousand persons | Average reach in thousands. |
| `SpotByBreaksAvReach000` | thousand persons | Average reach by ad-block significant time. |
| `SpotByProgramAvReach000` | thousand persons | Average reach by program significant time. |
| `AvReachPer` | % | Average reach in %. |
| `SpotByBreaksAvReachPer` | % | Average reach in % by ad-block significant time. |
| `SpotByProgramAvReachPer` | % | Average reach in % by program significant time. |
| `OTS000Avg` | thousand persons | Opportunity to See in thousands (Avg). |
| `OTSPerAvg` | % | Opportunity to See in % (Avg). |
| `SpotByBreaksOTS000Avg` | thousand persons | OTS in thousands by ad-block (Avg). |
| `SpotByBreaksOTSPerAvg` | % | OTS in % by ad-block (Avg). |
| `Rtg000Avg` | thousand persons | Rating in thousands (Avg). |
| `SalesRtg000Avg` | thousand persons | Billing rating in thousands (Avg). |
| `SpotByBreaksRtg000Avg` | thousand persons | Rating in thousands by ad-block (Avg). |
| `SpotByBreaksSalesRtg000Avg` | thousand persons | Billing rating in thousands by ad-block (Avg). |
| `SpotByProgramRtg000Avg` | thousand persons | Rating in thousands by program (Avg). |
| `RtgPerAvg` | % | Rating in % (Avg). |
| `StandRtgPerAvg` | % | Standard rating % (Avg). |
| `SalesRtgPerAvg` | % | Sales rating % (Avg). |
| `StandSalesRtgPerAvg` | % | Standard sales rating % (Avg). |
| `SpotByBreaksRtgPerAvg` | % | Rating % by ad-block (Avg). |
| `SpotByBreaksStandRtgPerAvg` | % | Standard rating % by ad-block (Avg). |
| `SpotByBreaksSalesRtgPerAvg` | % | Sales rating % by ad-block (Avg). |
| `SpotByBreaksStandSalesRtgPerAvg` | % | Standard sales rating % by ad-block (Avg). |
| `SpotByProgramRtgPerAvg` | % | Rating % by program (Avg). |
| `Frequency` | contacts | Average number of contacts per viewer. |
| `SpotByBreaksFrequency` | contacts | Frequency by ad-block significant time. |
| `ShareAvg` | % | Audience share (Avg). |
| `TgAffinPerAvg` | index | Average affinity index. |
| `ATVAvg` | minutes | Average viewing time among population (Avg). |
| `ATVReachAvg` | minutes | Average viewing time among viewers (Avg). |
| `Universe000Avg` | thousand persons | Total population size (Avg). |
| `SampleAvg` | respondents | Sample size (Avg). |
| `TTVRtg000Avg` | thousand persons | Total TV rating in thousands (Avg). |
| `TTVRtgPerAvg` | % | Total TV rating in % (Avg). |
| `DurationAvg` | seconds | Average event duration (Avg). |

#### Slices

Crosstab slices are kit-dependent (loaded via `get_crosstab_unit`). The docs Crosstab filter/slice page returned 404 — see [§21 Content Gaps](#21-content-gaps--undocumented-parameters). Only Timeband and Simple doc pages resolve; Crosstab filter set = Simple filter set (verified from `build_*_task` signatures).

> **Last verified: 2026-10-28 — sources: [url_1], [url_3]**

### 4.4 Consumption Target

> **Metadata:** `api_task_type` = `consumption-target` · `api_endpoint` = `/task/consumption-target` · `user_face_name` = "Отчет Consumption Target" · `library_builder` = `build_consumption_target_task` / `cats.get_consumption_target_unit(kit_id)` · `applies_to` = TV, TV_REG · `options_defaults` = `{kitId: 1, viewingSubject: "RESPONDENT"}` · `sortings_supported` = ❌ · `scales_supported` = ✅ · `custom_respondent_variable_id_supported` = ✅

- **Common filters:** [COMMON](#common-parameters-all-report-types)
- **Time-specific:** `time_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Program/event filters:** `program_filter`, `break_filter`, `ad_filter`, `subject_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Duration filter:** `duration_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Custom variables:** `custom_respondent_variable_id`, `custom_demo_variable_id`, `custom_company_variable_id` ✅
- **Consumption-target-only:** `consumption_target_options` (see [§16](#16-consumption-target-options))
- **Sortings:** ❌ not supported
- **Frequency distribution:** — not applicable

#### Statistics (4)

| Statistic | Unit | Description (EN) |
|---|---|---|
| `CommonWatchers` | respondents | Viewers who watched the channel ≥1 min during the period. |
| `CommonNotWatchers` | respondents | Non-viewers (did not watch the channel during the period). |
| `NGroupResp` | respondents | Viewers sorted by viewing duration into n equal-by-respondent groups; count via `consumption_target_options.groupsCount`. |
| `NGroupDur` | respondents | Viewers sorted by viewing duration into n groups; count via `consumption_target_options.groupsCount`. |

#### Slices

Kit-dependent; loaded via `get_consumption_target_unit(kit_id)`. Docs page 404 — see [§21](#21-content-gaps--undocumented-parameters).

> **Last verified: 2026-10-28 — sources: [url_1], [url_3]**

### 4.5 Duplication Timeband

> **Metadata:** `api_task_type` = `duplication-timeband` · `api_endpoint` = `/task/duplication-timeband` · `user_face_name` = "Отчет Пересечение аудитории (Duplication Timeband)" · `library_builder` = `build_duplication_timeband_task` / `cats.get_duplication_timeband_unit(kit_id)` · `applies_to` = TV, TV_REG · `options_defaults` = `{kitId: 1, viewingSubject: "RESPONDENT"}` · `sortings_supported` = ❌ · `scales_supported` = ✅

- **Common filters:** [COMMON](#common-parameters-all-report-types)
- **Time-specific:** `time_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Duration filter:** `duration_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Duplication filters:** `duplication_company_filter`, `duplication_time_filter` ✅ ([Matrix](#3-filter-support-matrix))
- **Custom variables:** `custom_duplication_time_variable_id`, `custom_duplication_company_variable_id` ✅
- **Sortings:** ❌ not supported
- **Frequency distribution:** — not applicable

#### Statistics (4)

| Statistic | Unit | Description (EN) |
|---|---|---|
| `CumDupl000` | thousand persons | Cumulative audience overlap in thousands. |
| `CumDuplPer` | % | Cumulative overlap in % of the main medium's accumulated reach. |
| `DuplAvReach000` | thousand persons | Average daily audience overlap in thousands. |
| `DuplAvReachPer` | % | Average daily overlap in % of the main medium's average daily reach. |

#### Slices

Kit-dependent; loaded via `get_duplication_timeband_unit(kit_id)`. Docs page 404 — see [§21](#21-content-gaps--undocumented-parameters).

> **Last verified: 2026-10-28 — sources: [url_1], [url_3]**

### 4.6 Respondent Analysis [UNDOCUMENTED]

> **Metadata:** `api_task_type` = `respondent-analysis` · `api_endpoint` = `/task/respondent-analysis` · `user_face_name` = "Анализ отдельных респондентов (Respondent analysis)" · `library_builder` = **NONE** (no `build_respondent_analysis_task` convenience method) · `applies_to` = TV Index only · `status` = UNDOCUMENTED

- **Status:** ⚠️ Entirely undocumented in the vendor docs URLs and `_help.ipynb` notebook.
- **Library support:** `task_urls` and `check_task.task_types` reference `respondent-analysis`; `get_respondent_analysis_unit(kit_id)` exists in `catalogs.py` (line 4944) but is never mentioned in the notebook or any source URL.
- **Usage:** To use it, call `build_task(task_type='respondent-analysis', ...)` directly (the library validates `task_type` only as a `str`).
- **Filters:** Kit-dependent; loaded via `get_respondent_analysis_unit(kit_id)`. Not documented.
- **Statistics:** Kit-dependent; loaded via `get_respondent_analysis_unit(kit_id)`. Not documented.
- **Options:** `{kitId: 1}`
- **Common filters:** Assumed identical to other report types, but **unverified**.
- **Sortings:** Unknown.
- **Custom variables:** Unknown.

> **Last verified: 2026-10-28 — sources: vendor_library v1.8.3 (catalogs.py:4944, tasks.py task_urls, checks.py task_types)**

---

## Statistics Dictionary

Global alphabetical lookup of all statistic names across all report types. Per-report-type tables ([§4](#4-per-report-type-sections)) list only applicable statistics with links here.

| Statistic | Unit | Scope | Report Types | Description (EN) |
|---|---|---|---|---|
| `ATVAvg` | minutes | — | Crosstab | Average viewing time among population (Avg). |
| `ATVReach` | minutes | — | Simple | Average viewing time among viewers for one event. |
| `ATVReachAvg` | minutes | — | Crosstab | Average daily viewing time among viewers (Avg). |
| `ATVReachSum` | minutes | — | Crosstab | Average daily viewing time among viewers (Sum). |
| `ATVSum` | minutes | — | Crosstab | Average daily viewing time among population (Sum). |
| `ATV` | minutes | — | Simple | Average viewing time among population for one event. Also in Timeband. |
| `AvMReach000` | thousand persons | month | Timeband | Average monthly reach in thousands. |
| `AvMReachPer` | % | month | Timeband | Average monthly reach in %. |
| `AvReach000` | thousand persons | day | Timeband, Crosstab | Average daily reach in thousands (≥1 min/day). |
| `AvReachPer` | % | day | Timeband, Crosstab | Average daily reach in %. |
| `AvReachPer` | % | — | Timeband | Average daily reach in %. |
| `AvWkReach000` | thousand persons | week | Timeband | Average weekly reach in thousands (≥1 min/week). |
| `AvWkReachPer` | % | week | Timeband | Average weekly reach in %. |
| `ConsolidatedCostRUB` | RUB | — | Simple | Consolidated placement cost by ad-block assessment type (RUB). |
| `ConsolidatedCostSumRUB` | RUB | — | Crosstab | Consolidated placement cost (Sum). |
| `ConsolidatedCostSumUSD` | USD | — | Crosstab | Consolidated placement cost (Sum). |
| `ConsolidatedCostUSD` | USD | — | Simple | Consolidated placement cost by ad-block assessment type (USD). |
| `CostByGRPRUB` | RUB | — | Simple | Placement cost per GRP pricelists (RUB). |
| `CostByGRPSumRUB` | RUB | — | Crosstab | Placement cost per GRP (Sum). |
| `CostByGRPSumUSD` | USD | — | Crosstab | Placement cost per GRP (Sum). |
| `CostByGRPUSD` | USD | — | Simple | Placement cost per GRP pricelists (USD). |
| `CostByMinRUB` | RUB | — | Simple | Placement cost from ad protocol per minute pricelists (RUB). |
| `CostByMinSumRUB` | RUB | — | Crosstab | Placement cost per minute (Sum). |
| `CostByMinSumUSD` | USD | — | Crosstab | Placement cost per minute (Sum). |
| `CostByMinUSD` | USD | — | Simple | Placement cost per minute pricelists (USD). |
| `CumDupl000` | thousand persons | — | Duplication Timeband | Cumulative audience overlap in thousands. |
| `CumDuplPer` | % | — | Duplication Timeband | Cumulative overlap in % of main medium's accumulated reach. |
| `CumReach000` | thousand persons | — | Crosstab | Cumulative reach in thousands. |
| `CumReachPer` | % | — | Crosstab | Cumulative reach in %. |
| `DuplAvReach000` | thousand persons | — | Duplication Timeband | Average daily audience overlap in thousands. |
| `DuplAvReachPer` | % | — | Duplication Timeband | Average daily overlap in % of main medium's average daily reach. |
| `Duration` | seconds | — | Simple | Actual (unrounded) event duration in seconds. |
| `DurationAvg` | seconds | — | Crosstab | Average event duration (Avg). |
| `DurationSum` | seconds | — | Crosstab | Total event duration (Sum). |
| `Fidelity` | % | — | Simple | Average viewing time / event duration. |
| `Frequency` | contacts | — | Simple, Crosstab | Average number of contacts per viewer. |
| `FrequencyDist000` | thousand persons | — | Simple, Crosstab | Frequency distribution of reach in thousands. Configured via `frequency_dist_conditions`. |
| `FrequencyDistPer` | % | — | Simple, Crosstab | Frequency distribution of reach in %. |
| `OTSPerAvg` | % | — | Crosstab | Opportunity to See in % (Avg). |
| `OTSPerSum` | % | — | Crosstab | Opportunity to See in % (Sum). |
| `OTS000Avg` | thousand persons | — | Crosstab | Opportunity to See in thousands (Avg). |
| `OTS000Sum` | thousand persons | — | Crosstab | Opportunity to See in thousands (Sum). |
| `Quantity` | events | — | Simple | Number of broadcast events. |
| `QuantitySum` | events | — | Crosstab | Number of events (Sum). |
| `Reach000` | thousand persons | period / event | Timeband, Simple, Crosstab | Accumulated reach in thousands. Watched ≥1 min. |
| `ReachPer` | % | period / event | Timeband, Simple, Crosstab | Accumulated reach in %. |
| `Rtg000` | thousand persons | — | Timeband, Simple, Crosstab | Average target-audience viewers. |
| `Rtg000Avg` | thousand persons | — | Crosstab | Rating in thousands (Avg). |
| `Rtg000Sum` | thousand persons | — | Crosstab | Rating in thousands (Sum). |
| `RtgPer` | % | — | Timeband, Simple, Crosstab | Rating in % of total audience size. |
| `RtgPerAvg` | % | — | Crosstab | Rating in % (Avg). |
| `RtgPerSum` | % | — | Crosstab | Rating in % (Sum). |
| `SalesAffinPer` | index | — | Simple | Affinity index vs channel billing audience. |
| `SalesRtg000` | thousand persons | — | Simple | Average viewers in channel's billing audience. |
| `SalesRtg000Avg` | thousand persons | — | Crosstab | Billing rating in thousands (Avg). |
| `SalesRtg000Sum` | thousand persons | — | Crosstab | Billing rating in thousands (Sum). |
| `SalesRtgPer` | % | — | Simple | Rating % by channel billing audience. |
| `SalesRtgPerAvg` | % | — | Crosstab | Sales rating % (Avg). |
| `SalesRtgPerSum` | % | — | Crosstab | Sales rating % (Sum). |
| `Sample` | respondents | — | Timeband, Simple | Sample size (respondents). |
| `SampleAvg` | respondents | — | Crosstab | Sample size (Avg). |
| `Share` | % | interval | Timeband, Simple | Audience share. |
| `ShareAvg` | % | — | Crosstab | Audience share (Avg). |
| `SpotByBreaksAccumulativeReach000` | thousand persons | — | Simple | Cumulative reach by ad-block significant time. |
| `SpotByBreaksAccumulativeReachPer` | % | — | Simple | Cumulative reach in % by ad-block significant time. |
| `SpotByBreaksAvReach000` | thousand persons | — | Crosstab | Average reach by ad-block significant time. |
| `SpotByBreaksAvReachPer` | % | — | Crosstab | Average reach in % by ad-block significant time. |
| `SpotByBreaksCumReach000` | thousand persons | — | Crosstab | Cumulative reach by ad-block significant time. |
| `SpotByBreaksCumReachPer` | % | — | Crosstab | Cumulative reach in % by ad-block significant time. |
| `SpotByBreaksFrequency` | contacts | — | Crosstab | Frequency by ad-block significant time. |
| `SpotByBreaksFrequencyDist000` | thousand persons | — | Simple, Crosstab | Frequency distribution by ad-block significant time. |
| `SpotByBreaksFrequencyDistPer` | % | — | Simple, Crosstab | Frequency distribution by ad-block (Sum). |
| `SpotByBreaksOTSPerAvg` | % | — | Crosstab | OTS in % by ad-block (Avg). |
| `SpotByBreaksOTSPerSum` | % | — | Crosstab | OTS in % by ad-block (Sum). |
| `SpotByBreaksOTS000Avg` | thousand persons | — | Crosstab | OTS in thousands by ad-block (Avg). |
| `SpotByBreaksOTS000Sum` | thousand persons | — | Crosstab | OTS in thousands by ad-block (Sum). |
| `SpotByBreaksRtg000Avg` | thousand persons | — | Crosstab | Rating in thousands by ad-block (Avg). |
| `SpotByBreaksRtg000Sum` | thousand persons | — | Crosstab | Rating in thousands by ad-block (Sum). |
| `SpotByBreaksRtgPerAvg` | % | — | Crosstab | Rating % by ad-block significant time (Avg). |
| `SpotByBreaksRtgPerSum` | % | — | Crosstab | Rating % by ad-block (Sum). |
| `SpotByBreaksSalesRtg000Avg` | thousand persons | — | Crosstab | Billing rating in thousands by ad-block (Avg). |
| `SpotByBreaksSalesRtg000Sum` | thousand persons | — | Crosstab | Billing rating in thousands by ad-block (Sum). |
| `SpotByBreaksSalesRtgPerAvg` | % | — | Crosstab | Sales rating % by ad-block (Avg). |
| `SpotByBreaksSalesRtgPerSum` | % | — | Crosstab | Sales rating % by ad-block (Sum). |
| `SpotByBreaksStandRtgPerAvg` | % | — | Crosstab | Standard rating % by ad-block (Avg). |
| `SpotByBreaksStandRtgPerSum` | % | — | Crosstab | Standard rating % by ad-block (Sum). |
| `SpotByBreaksStandSalesRtgPerAvg` | % | — | Crosstab | Standard sales rating % by ad-block (Avg). |
| `SpotByBreaksStandSalesRtgPerSum` | % | — | Crosstab | Standard sales rating % by ad-block (Sum). |
| `SpotByBreaksTgSatReachPer` | index | — | Simple | Demo profile by reach, ad-block significant time. |
| `SpotByProgramAvReach000` | thousand persons | — | Crosstab | Average reach by program significant time. |
| `SpotByProgramAvReachPer` | % | — | Crosstab | Average reach in % by program significant time. |
| `SpotByProgramCumReach000` | thousand persons | — | Crosstab | Cumulative reach by program significant time. |
| `SpotByProgramCumReachPer` | % | — | Crosstab | Cumulative reach in % by program significant time. |
| `SpotByProgramRtg000Avg` | thousand persons | — | Crosstab | Rating in thousands by program (Avg). |
| `SpotByProgramRtg000Sum` | thousand persons | — | Crosstab | Rating in thousands by program (Sum). |
| `SpotByProgramRtgPer` | % | — | Simple | Rating % by program significant time. |
| `SpotByProgramRtgPerAvg` | % | — | Crosstab | Rating % by program significant time (Avg). |
| `SpotByProgramRtgPerSum` | % | — | Crosstab | Rating % by program significant time (Sum). |
| `SpotByProgramReach000` | thousand persons | — | Simple | Reach in thousands, by program significant time. |
| `SpotByProgramReachPer` | % | — | Simple | Reach in %, by program significant time. |
| `SpotByBreaksReach000` | thousand persons | — | Simple | Reach in thousands, by ad-block significant time. |
| `SpotByBreaksReachPer` | % | — | Simple | Reach in %, by ad-block significant time. |
| `StandRtgPer` | % | — | Simple | Standard rating % = Rtg% × expectedDuration / 20. |
| `StandRtgPerAvg` | % | — | Crosstab | Standard rating % (Avg). |
| `StandRtgPerSum` | % | — | Crosstab | Standard rating % (Sum). |
| `StandSalesRtgPer` | % | — | Simple | Standard sales rating % = SalesRtg% × expectedDuration / 20. |
| `StandSalesRtgPerAvg` | % | — | Crosstab | Standard sales rating % (Avg). |
| `StandSalesRtgPerSum` | % | — | Crosstab | Standard sales rating % (Sum). |
| `TAud` | minutes | — | Simple | Total viewing time for all viewers for one event. Also in Timeband. |
| `TAudSum` | minutes | — | Crosstab | Total viewing time (Sum). |
| `TgAffinPer` | index | — | Simple | Affinity index = event Rtg(target) / event Rtg(base). Also in Timeband. |
| `TgAffinPerAvg` | index | — | Crosstab | Average affinity index. |
| `TgAffinReachPer` | index | — | Simple | Affinity index based on reach. Also in Timeband. |
| `TgSatPer` | index | — | Simple | Demo profile = avg target Rtg(000) / avg base Rtg(000). Also in Timeband. |
| `TgSatReachPer` | index | — | Simple | Demo profile by reach. Also in Timeband. |
| `TgSatReachPer` | index | — | Timeband | Demo profile by reach. |
| `TTVATV` | minutes | — | Timeband | Total TV average daily viewing time among population. |
| `TTVATVReach` | minutes | — | Timeband | Total TV average daily viewing time among viewers. |
| `TTVAvReach000` | thousand persons | — | Timeband | Total TV average daily reach in thousands. |
| `TTVAvReachPer` | % | — | Timeband | Total TV average daily reach in %. |
| `TTVReach000` | thousand persons | — | Timeband | Total TV accumulated reach in thousands. |
| `TTVReachPer` | % | — | Timeband | Total TV accumulated reach in %. |
| `TTVRtg000` | thousand persons | — | Timeband | Total TV rating in thousands (vs Total channels). |
| `TTVRtg000Avg` | thousand persons | — | Crosstab | Total TV rating in thousands (Avg). |
| `TTVRtgPer` | % | — | Timeband | Total TV rating in %. |
| `TTVRtgPerAvg` | % | — | Crosstab | Total TV rating in % (Avg). |
| `Universe000` | thousand persons | — | Timeband, Simple | Total population size of target audience. |
| `Universe000Avg` | thousand persons | — | Crosstab | Total population size (Avg). |
| `WAgeAverage` | years | — | Timeband | Weighted average audience age. |

> ⚠️ **Note:** `WAgeAverage` appears in the URL-1 Timeband statistics list but is absent from the notebook's Timeband statistics cell — verified in `research_vendor_2.json` discrepancies.

> **Last verified: 2026-10-28 — sources: [url_1], [url_3]**

---

## Slices Dictionary

Cross-reference table of all slice element names. Each `Id` element typically also has `Name` and `EName` variants. The `en_to_ru` function (`utils.py:341-351`) translates `EName` → `Name` by string replacement when `data_lang = "ru"`.

> **Constraint:** At most 1 time slice per task among `researchDate` / `researchWeek` / `researchMonth`.

### Time Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `researchDate` | — | Time | Day-level research date. |
| `researchWeek` | — | Time | Week-level research week. |
| `researchMonth` | — | Time | Month-level research month. |
| `researchQuarter` | — | Time | Quarter-level research quarter. |
| `researchYear` | — | Time | Year-level research year. |
| `researchHalfYear` | — | Time | Half-year-level research period. |

### Weekday & Day-Type Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `researchWeekDay` | — | Weekday | Research weekday. |
| `researchDayType` | `+Name`, `+EName` | Day-type | Day type ID and name variants. |

### Location Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `locationId` | `+Name`, `+EName` | Location | Viewing location ID and name variants. |

### Channel Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `tvCompanyId` | `+Name`, `+EName` | Channel | TV company ID and name variants. |
| `tvThematicId` | `+Name`, `+EName` | Channel | Thematic ID and name variants. |
| `tvNetId` | `+Name`, `+EName` | Channel | TV network ID and name variants. |
| `regionId` | `+Name`, `+EName` | Channel | Region ID and name variants. |
| `tvCompanyHoldingId` | `+Name`, `+EName` | Channel | Company holding ID and name variants. |
| `tvCompanyMediaHoldingId` | `+Name`, `+EName` | Channel | Company media holding ID and name variants. |

### Timeband-Only Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `timeBand1` | — | Timeband | 1-minute time band. |
| `timeBand5` | — | Timeband | 5-minute time band. |
| `timeBand10` | — | Timeband | 10-minute time band. |
| `timeBand15` | — | Timeband | 15-minute time band. |
| `timeBand30` | — | Timeband | 30-minute time band. |
| `timeBand60` | — | Timeband | 60-minute time band. |

### Platform & Playback Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `platformId` | `+Name`, `+EName` | Platform | Platform ID and name variants. |
| `playBackTypeId` | `+Name`, `+EName` | Playback | Playback type ID and name variants. |

### Demographic Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `sex` | — | Demo | Sex. |
| `age` | — | Demo | Age. |
| `education` | — | Demo | Education level. |
| `work` | — | Demo | Employment type. |
| `persNum` | — | Demo | Number of persons in household. |
| `spendingsOnFood` | — | Demo | Food spending. |
| `tvNum` | — | Demo | Number of TVs in household. |
| `ageGroup` | — | Demo | Age group. |
| `incomeGroupRussia` | — | Demo | Income group (Russia kit). |
| `incomeGroup` | — | Demo | Income group (Cities kit, kitId=3). |
| `housewife` | — | Demo | Housewife flag. |
| `incomeEarner` | — | Demo | Income earner flag. |
| `incLevel` | — | Demo | Income level. |
| `kidsNum` | — | Demo | Number of children. |
| `kidsAge1`–`kidsAge7` | — | Demo | Child age 1–7. |
| `kidsAgeC1`–`kidsAgeC4` | — | Demo | Child age category (Cities kit). |
| `occupation` | — | Demo | Occupation (Cities kit). |
| `tradeIndustry` | — | Demo | Trade industry (Cities kit). |
| `status` | — | Demo | Social status. |
| `business` | — | Demo | Business status. |
| `enterprise` | — | Demo | Enterprise. |
| `property` | — | Demo | Property ownership. |
| `maritalStatus` | — | Demo | Marital status. |
| `lifeCycle` | — | Demo | Life cycle stage. |
| `internet` | — | Demo | Internet usage. |
| `dacha` | — | Demo | Dacha ownership. |
| `federalOkrug` | — | Demo | Federal district. |
| `incomeScale201401` | — | Demo | Income scale. |
| `equipmentV2` | — | Demo | Equipment type. |
| `wghSuburbAgeGroup` | — | Demo | Suburban area age group. |
| `wghDachaTvExist` | — | Demo | Dacha TV existence. |
| `smartTvYesNo` | — | Demo | Smart TV flag. |
| `wghVideoGameWoAge` | — | Demo | Video game usage. |
| `wghUsbTvHh` | — | Demo | USB TV household. |
| `cubeCity` | — | Demo | Cube city. |
| `timezone0` | — | Demo | Timezone. |
| `cube100Plus100Minus` | — | Demo | 100+ population ratio. |
| `sputnikTv` | — | Demo | Satellite TV. |
| `geoFrom082020` | — | Demo | Geo from 08.2020. |
| `video` | — | Demo | Video usage. |
| `city` | — | Demo | City. |

### Program Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `programId` | `+Name`, `+EName` | Program | Program ID and name variants. |
| `programName` | `+EName` | Program | Program name (English). |
| `programExtendedName` | `+EName` | Program | Extended program name (English). |
| `programGroupId` | `+Name`, `+EName` | Program | Program group ID and names. |
| `programTypeId` | `+Name`, `+EName` | Program | Program type ID and names. |
| `programCountryId` | `+Name`, `+EName` | Program | Program country ID and names. |
| `programCategoryId` | `+Name`, `+EName` | Program | Program category ID and names. |
| `programCategoryShortName` | `+EName` | Program | Short category name (English). |
| `programSportId` | `+Name`, `+EName` | Program | Sport ID and names. |
| `programSportGroupId` | `+Name`, `+EName` | Program | Sport group ID and names. |
| `programProducerId` | `+Name`, `+EName` | Program | Producer ID and names. |
| `programProducerCountryId` | `+Name`, `+EName` | Program | Producer country ID and names. |
| `programLanguageId` | `+Name`, `+EName` | Program | Language ID and names. |
| `programAgeRestrictionId` | `+Name`, `+EName` | Program | Age restriction ID and names. |
| `programIsChild` | `+Name`, `+EName` | Program | Child program flag. |
| `programIsLive` | `+Name`, `+EName` | Program | Live broadcast flag. |
| `programProducerYear` | — | Program | Producer year. |
| `programStartTime` / `programFinishTime` | — | Program | Start/finish times. |
| `programDuration` | — | Program | Duration in seconds. |
| `programSpotId` | — | Program | Spot ID. |
| `programBreaksCount` | — | Program | Breaks count. |
| `programFirstIssueDate` | — | Program | First issue date. |
| `programNotes` | — | Program | Program notes. |
| `programSpotIdOriginal` | — | Program | Original spot ID. |
| `programDateOriginal` | — | Program | Original date. |
| `programStartTimeOriginal` | — | Program | Original start time. |
| `programTvCompanyOriginal` | — | Program | Original TV company. |

### Break Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `breaksId` | `+Name`, `+EName` | Break | Break ID and names. |
| `breaksName` | `+EName` | Break | Break name (English). |
| `breaksStyleId` | `+Name`, `+EName` | Break | Break style ID and names. |
| `breaksDistributionType` | `+Name`, `+EName` | Break | Distribution type ID and names. |
| `breaksPositionType` | `+Name`, `+EName` | Break | Position type ID and names. |
| `breaksContentType` | `+Name`, `+EName` | Break | Content type ID and names. |
| `breaksPosition` | — | Break | Position in program. |
| `breaksStartTime` / `breaksFinishTime` | — | Break | Start/finish times. |
| `breaksDuration` | — | Break | Duration in seconds. |
| `breaksSpotId` | — | Break | Spot ID. |
| `grpTypeId` | `+Name`, `+EName` | Break | GRP type ID and names. |
| `price` / `priceRub` | — | Break | Price. |
| `grpPrice` / `grpPriceRub` | — | Break | GRP price. |
| `grpCost` / `grpCostRub` | — | Break | GRP cost. |
| `breaksAdCount` | — | Break | Ad count in break. |
| `breaksPrimeTimeStatusId` | `+Name`, `+EName` | Break | Prime time status ID and names. |
| `breaksIssueStatusId` | `+Name`, `+EName` | Break | Issue status ID and names. |
| `breaksNotes` | — | Break | Break notes. |
| `breaksSpotIdOriginal` | — | Break | Original spot ID. |
| `breaksDateOriginal` | — | Break | Original date. |
| `breaksStartTimeOriginal` | — | Break | Original start time. |
| `breaksTvCompanyOriginal` | — | Break | Original TV company. |

### Ad Slices

| Slice | Variants | Category | Description |
|---|---|---|---|
| `adId` | `+Name`, `+EName` | Ad | Ad ID and names. |
| `adTypeName` | `+EName` | Ad | Ad type name (English). |
| `adStyleId` | `+Name`, `+EName` | Ad | Ad style ID and names. |
| `adStandardDuration` | — | Ad | Standard duration. |
| `adSpotId` | — | Ad | Spot ID. |
| `adFirstIssueDate` | — | Ad | First issue date. |
| `adSloganAudioId` | `+Name` | Ad | Slogan audio ID + name. |
| `adSloganVideoId` | `+Name` | Ad | Slogan video ID + name. |
| `adNotes` | — | Ad | Ad notes. |
| `adIssueStatusId` | `+Name`, `+EName` | Ad | Issue status ID and names. |
| `adDistributionType` | `+Name`, `+EName` | Ad | Distribution type ID and names. |
| `advertiserId` | `+Name`, `+EName` | Ad | Advertiser ID and names. |
| `brandId` | `+Name`, `+EName` | Ad | Brand ID and names. |
| `subbrandId` | `+Name`, `+EName` | Ad | Subbrand ID and names. |
| `modelId` | `+Name`, `+EName` | Ad | Model ID and names. |
| `articleLevel1Id` | `+Name`, `+EName` | Ad | Article level 1 ID and names. |
| `articleLevel2Id` | `+Name`, `+EName` | Ad | Article level 2 ID and names. |
| `articleLevel3Id` | `+Name`, `+EName` | Ad | Article level 3 ID and names. |
| `articleLevel4Id` | `+Name`, `+EName` | Ad | Article level 4 ID and names. |
| `advertiserListId` | `+Name`, `+EName` | Ad | Advertiser list ID and names. |
| `brandListId` | `+Name`, `+EName` | Ad | Brand list ID and names. |
| `subbrandListId` | `+Name`, `+EName` | Ad | Subbrand list ID and names. |
| `modelListId` | `+Name`, `+EName` | Ad | Model list ID and names. |
| `articleList2Id` | `+Name`, `+EName` | Ad | Article list 2 ID and names. |
| `articleList3Id` | `+Name`, `+EName` | Ad | Article list 3 ID and names. |
| `articleList4Id` | `+Name`, `+EName` | Ad | Article list 4 ID and names. |
| `adAgeRestrictionId` | `+Name`, `+EName` | Ad | Age restriction ID and names. |
| `adPositionType` | `+Name`, `+EName` | Ad | Position type ID and names. |
| `adPositionId` | — | Ad | Position ID. |
| `adPrimeTimeStatusId` | `+Name`, `+EName` | Ad | Prime time status ID and names. |
| `adTvAreaId` | `+Name`, `+EName` | Ad | TV area ID and names. |
| `advertiserTvAreaId` | `+Name`, `+EName` | Ad | Advertiser TV area ID and names. |
| `brandTvAreaId` | `+Name`, `+EName` | Ad | Brand TV area ID and names. |
| `subbrandTvAreaId` | `+Name`, `+EName` | Ad | Subbrand TV area ID and names. |
| `modelTvAreaId` | `+Name`, `+EName` | Ad | Model TV area ID and names. |
| `adIssueDescriptionName` | — | Ad | Issue description name. |
| `qr` | `+Name`, `+EName` | Ad | QR code flag and names. |
| `qrDuration` | — | Ad | QR duration. |
| `adSpotIdOriginal` | — | Ad | Original spot ID. |
| `adDateOriginal` | — | Ad | Original date. |
| `adStartTimeOriginal` | — | Ad | Original start time. |
| `adTvCompanyOriginal` | — | Ad | Original TV company. |

> **Note:** `+Name` and `+EName` variants are produced by appending `Name` or `EName` to the base `Id` element. The `en_to_ru` function (`utils.py:341-351`) replaces `EName` → `Name` for Russian-language output.

> **Last verified: 2026-10-28 — sources: [url_2], [url_2b], [url_3]**

---

## Options Dictionary

The `options` dict (JSON key `options`) configures per-task calculation behavior. Subkeys are documented once here.

### kitId

| Parameter | JSON Key | Type | Default | Valid Values | Description | Templates |
|---|---|---|---|---|---|---|
| `kit_id` | `kitId` | `int` | 3 (TV_REG), 1 (TV runtime) | 1–10 | Dataset ID. Only one value can be specified. `kitId=3` selects the TV Index Cities dataset (TV_REG default). | All DATA_DEFAULTS; subtypes override to 7; report files override per entry |

**Kit ID Reference Table:**

| kitId | Name | Panel Type |
|---|---|---|
| 1 | TV Index All Russia | Standard panel |
| 2 | TV Index Russia 100+ | Standard panel |
| 3 | TV Index Cities | Cities panel |
| 4 | TV Index Plus All Russia | Propagated panel |
| 5 | TV Index Plus Russia 100+ | Propagated panel |
| 6 | TV Index Moscow | Standard panel |
| 7 | Big TV | Big TV dataset (7-kit) |
| 9 | TV Index Russia Special | Special |
| 10 | TV Index Moscow Special | Special |

> Standard panel `standRtg.standardDuration` default = 5; Propagated panel default = 50.

### Other Options Subkeys

| Subkey | Param | Type | Default | Valid Values | Description | Templates |
|---|---|---|---|---|---|---|
| `big_tv` | `bigTv` | `bool` | `false` | `true`, `false` | True => calculate by 7th kit (Big TV). | Documented; not used in any template. Big TV is selected via `kitId=7` instead. |
| `use_nbd` | `useNbd` | `bool` | `true` | `true`, `false` | Use NBD correction for accumulated reach. | Only in `nat_tv_brands_weekly_reach.json5` |
| `base_date` | `baseDate` | `str` | `null` | `YYYY-MM-DD` | Base day for accumulated reach. | Only in `nat_tv_brands_weekly_reach.json5` (`null`) |
| `base_date_calc_type` | `baseDateCalcType` | `str` | `BY_RESEARCH_PERIOD` | `BY_RESEARCH_PERIOD`, `BY_ISSUES` | Basis for base date: average day of calculation period or average day on which events occurred. | Only in `nat_tv_brands_weekly_reach.json5` |
| `one_base_date_per_each_date_range` | `oneBaseDatePerEachDateRange` | `bool` | `false` | `true`, `false` | True = each period-slice gets its own base day; False = all share the calendar-period average. | Only in `nat_tv_brands_weekly_reach.json5` |
| `total_type` | `totalType` | `str` | `TotalChannels` | `TotalChannels`, `TotalTVSet`, `TotalChannelsThem` | Basis for Share and TTV statistics. | Project files: `TotalTVSet` in `nat_tv_tvreport.json5` (NAT_TV_CHANNELS_* entries, TV_REG files) |
| `min_sample` | `minSmp` | `int` | `5` | Any positive int | Minimum sample size to display a value; cells below are masked with `*`. | Documented; not used in any template (API uses default 5). |
| `stand_rtg` | `standRtg` | `dict` | `{useRealDuration: false, standardDuration: 5}` | — | Subkeys: `useRealDuration` (bool, default `false`), `standardDuration` (int, default 5 / 50 for propagated panel). Affects `StandRtgPer`, `StandSalesRtgPer` and all `*Sum`/`*Avg` standard variants. | All DATA_DEFAULTS; subtypes and report files override |
| `issue_type` | `issueType` | `str` | `PROGRAM` | `PROGRAM`, `BREAKS`, `AD` | ⚠️ **Crosstab-specific** — selects the event aggregation grain. | TV_REG defaults (all `AD`); TV crosstab subtypes; report files |
| `viewing_subject` | `viewingSubject` | `str` | `RESPONDENT` | `RESPONDENT`, `HOUSEHOLD` | Unit of calculation. | Documented; not used in any template (API uses default `RESPONDENT`). |

> 📋 **Project-level options** (not TVI API parameters): `time_unit` (splits frequency when `date_filter` exceeds the API's ~366-day per-task limit), `last_time` (`{unit_count, include_current}`, "last N periods" convenience). These are handled by the project's `PeriodModel`, not sent to the API as options.

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (tasks.py)**

---

## Filter Element Reference

### Date & Period Elements

| Element | Dictionary Endpoint | Getter Method | Description |
|---|---|---|---|
| `researchDate` | — | — | Day-level research date. |
| `researchWeek` | — | — | Week-level research week. |
| `researchMonth` | — | — | Month-level research month. |
| `researchQuarter` | `/dictionary/tv/quarter` | `get_tv_quarter` | Quarter-level research quarter. |
| `researchYear` | — | — | Year-level research year. |
| `researchHalfYear` | — | — | Half-year-level research period. |
| `researchWeekDay` | `/dictionary/tv/day-week` | `get_tv_day_week` | Research weekday (1–7). |
| `researchDayType` | `/dictionary/tv/research-day-type` | `get_tv_research_day_type` | Day type (working / weekend / holiday). |

### Channel/Company Elements

| Element | Dictionary Endpoint | Getter Method | Description |
|---|---|---|---|
| `tvCompanyId` | `/dictionary/tv/company` | `get_tv_company` | TV company ID. |
| `tvCompanyEName` | `/dictionary/tv/company` | `get_tv_company` | TV company EName. |
| `tvThematicId` | `/dictionary/tv/thematic` | `get_tv_thematic` | Thematic group ID. |
| `tvNetId` | `/dictionary/tv/net` | `get_tv_net` | TV network ID. Used in `nat_tv_tvreport.json5` company_filter `tvNetId IN (...)`. |
| `regionId` | `/dictionary/tv/region` | `get_tv_region` | Region ID. Used in TV_REG `company_filter` via `TVRegTaskStrategy.generate` (reports.py:829). |
| `tvCompanyHoldingId` | `/dictionary/tv/company-holding` | `get_tv_company_holding` | Company holding ID. |
| `tvCompanyMediaHoldingId` | `/dictionary/tv/company-media-holding` | `get_tv_company_media_holding` | Company media holding ID. |

### Location Elements

| Element | Dictionary Endpoint | Getter Method | Description |
|---|---|---|---|
| `locationId` | `/dictionary/tv/location` | `get_tv_location` | Viewing location ID (home / non-home TV). |

### Demographic Elements

| Element | Dictionary Endpoint | Getter Method | Description |
|---|---|---|---|
| `sex` | — | `get_tv_demo_attribute` | Sex (1=male, 2=female). |
| `age` | — | `get_tv_demo_attribute` | Age in years. |
| `education` | — | `get_tv_demo_attribute` | Education level. |
| `incomeGroup` | — | `get_tv_demo_attribute` | Income group (Cities kit, kitId=3). |
| `incomeGroupRussia` | — | `get_tv_demo_attribute` | Income group (Russia kit, kitId=1). |
| … (see [§6 Slices Dictionary — Demographic Slices](#6-slices-dictionary) for full list of 48+ attributes) | | | Full list in §6. |

> The canonical list of demographic element names is in §6. First letter of each attribute is lowercase (e.g., `wghSuburbAgeGroup`). All demo attributes are usable in `basedemo_filter` / `targetdemo_filter` and as slices, retrieved via `get_tv_demo_attribute()`.

### Program Elements

| Element | Description |
|---|---|
| `programIsChild` | Child program flag. |
| `programProducerYear` | Producer year. |
| `programStartTime` / `programFinishTime` | Start/finish times. |
| `programDuration` | Duration in seconds. |
| `programSpotId` | Spot ID. |
| `programId` | Program ID. |
| `programGroupId` | Program group ID. |
| `programTypeId` | Program type ID. |
| `programCountryId` | Program country ID. |
| `programCategoryId` | Program category ID. |
| `programSportId` | Sport ID. |
| `programSportGroupId` | Sport group ID. |
| `programProducerId` | Producer ID. |
| `programIsLive` | Live broadcast flag. |
| `programFirstIssueDate` | First issue date. |
| `programLanguageId` | Language ID. |
| `programAgeRestrictionId` | Age restriction ID. |
| `programProducerCountryId` | Producer country ID. |
| `programBreaksCount` | Breaks count. |
| `programSpotIdOriginal` → `programTvCompanyOriginal` | Original metadata fields. |

### Break Elements

| Element | Description |
|---|---|
| `breaksStartTime` / `breaksFinishTime` | Start/finish times. |
| `breaksDuration` | Duration in seconds. |
| `breaksSpotId` / `breaksId` | Spot/break ID. |
| `breaksStyleId` | Style ID. |
| `breaksDistributionType` | Distribution type. |
| `breaksPositionType` | Position type. |
| `breaksContentType` | Content type. |
| `breaksPosition` / `breaksAdCount` | Position in program / ad count. |
| `grpTypeId` / `price` / `grpPrice` / `grpCost` | Pricing fields. |
| `breaksPrimeTimeStatusId` / `breaksIssueStatusId` | Status IDs. |
| `breaksSpotIdOriginal` → `breaksTvCompanyOriginal` | Original metadata fields. |

### Ad Elements

| Element | Description |
|---|---|
| `adStartTime` / `adFinishTime` | Start/finish times. |
| `adStandardDuration` | Standard duration. |
| `adSpotId` / `adId` | Spot/ad ID. |
| `adTypeId` / `adStyleId` | Type/style IDs. |
| `adFirstIssueDate` | First issue date. |
| `adSloganAudioId` / `adSloganVideoId` | Slogan IDs. |
| `adIssueStatusId` / `adDistributionType` | Status/distribution. |
| `advertiserId` / `brandId` / `subbrandId` / `modelId` | Brand hierarchy IDs. |
| `articleLevel1Id` → `articleLevel4Id` | Article level IDs. |
| `advertiserListId` → `articleList4Id` | List IDs. |
| `adAgeRestrictionId` / `adPositionType` / `adPositionId` | Restriction/position IDs. |
| `adPrimeTimeStatusId` / `adTvAreaId` | TV area and prime-time. |
| `advertiserTvAreaId` → `modelTvAreaId` | TV area per brand level. |
| `qr` / `qrDuration` | QR code flag/duration. |
| `adSpotIdOriginal` → `adTvCompanyOriginal` | Original metadata fields. |

### Platform & Playback Elements

| Element | Dictionary Endpoint | Getter Method | Description |
|---|---|---|---|
| `platformId` | `/dictionary/tv/platform` | `get_tv_platform` | Platform ID (e.g., linear TV, OTT). |
| `playBackTypeId` | `/dictionary/tv/play-back-type` | `get_tv_play_back_type` | Playback type ID. |

### Big TV Elements

Big TV (kitId=7) enables the `bigtv_filter` (`filter.bigTvFilter`). Big TV-specific statistics are documented on a child page (URL-1 §4.1 "Статистики набора данных Big TV") which was not fetched during research — see [§21 Content Gaps](#21-content-gaps--undocumented-parameters).

### Dictionary Reference

| Dictionary Endpoint | Getter Method | Description |
|---|---|---|
| `/dictionary/tv/day-week` | `get_tv_day_week` | Research weekday. |
| `/dictionary/tv/research-day-type` | `get_tv_research_day_type` | Day type. |
| `/dictionary/tv/location` | `get_tv_location` | Viewing location. |
| `/dictionary/tv/demo-attribute` | `get_tv_demo_attribute` | Demographic attributes. |
| `/dictionary/tv/company` | `get_tv_company` | TV companies. |
| `/dictionary/tv/thematic` | `get_tv_thematic` | Thematic groups. |
| `/dictionary/tv/net` | `get_tv_net` | TV networks. |
| `/dictionary/tv/region` | `get_tv_region` | Regions. Used by TV_REG for city-based reporting. |
| `/dictionary/tv/company-holding` | `get_tv_company_holding` | Company holdings. |
| `/dictionary/tv/company-media-holding` | `get_tv_company_media_holding` | Media holdings. |
| `/dictionary/tv/program` | — | Programs. |
| `/dictionary/tv/program-group` → `/dictionary/tv/program-producer-country` | — | Program metadata. |
| `/dictionary/tv/language` | — | Languages. |
| `/dictionary/tv/age-restriction` | — | Age restrictions. |
| `/dictionary/tv/breaks` → `/dictionary/tv/breaks-content` | — | Break metadata. |
| `/dictionary/tv/issue-status` | — | Issue status. |
| `/dictionary/tv/ad` → `/dictionary/tv/ad-type` | — | Ad metadata. |
| `/dictionary/tv/advertiser` → `/dictionary/tv/subbrand` | — | Brand hierarchy. |
| `/dictionary/tv/model` → `/dictionary/tv/article` | — | Models and articles. |
| `/dictionary/tv/advertiser-list` → `/dictionary/tv/article-list4` | — | List references. |
| `/dictionary/tv/ad-position` | — | Ad positions. |
| `/dictionary/tv/area` | — | TV areas. |
| `/dictionary/tv/platform` | `get_tv_platform` | Platforms. |
| `/dictionary/tv/play-back-type` | `get_tv_play_back_type` | Playback types. |
| `/dictionary/tv/grp-type` | — | GRP types. |
| `/dictionary/tv/no-yes-na` | — | Yes/no/NA (QR). |

> **Last verified: 2026-10-28 — sources: [url_2], [url_2b], [url_3], vendor_library v1.8.3 (catalogs.py)**

---

## Filter Operators & Expression Syntax

The TVI API uses a string-based expression language for filter values.

### Operators

| Operator | Meaning |
|---|---|
| `IN` | Include in enumeration. |
| `NIN` | Exclude from enumeration. |
| `EQ` | Equal (`=`). |
| `NEQ` | Not equal (`<>`). |
| `LT` | Less than (`<`). |
| `LTE` | Less than or equal (`<=`). |
| `GT` | Greater than (`>`). |
| `GTE` | Greater than or equal (`>=`). |

### Between-Operators

| Operator | Meaning |
|---|---|
| `AND` | Logical AND between conditions. |
| `OR` | Logical OR between conditions. |

### Special Format Notes

- **`*` = required** (vendor docs marker); `**` = required for Big TV (7-kit) only.
- **If a filter is unused, pass `null`** — do NOT delete or comment it out (may cause unexpected results or errors).
- **`time_filter` format:** HHMMSS integers; right boundary is **EXCLUSIVE**. Example: `timeBand1 >= 70000 AND timeBand1 < 110000` = 07:00–11:00.
- **`date_filter` format:** `list[tuple[str, str]]` of `(start, end)` tuples as `'YYYY-MM-DD'`; multiple periods allowed.
- **`region_filter` ⚠️ DEPRECATED:** The vendor's `checks.check_task` prints: *"regionFilter в настоящее время не используется. Используйте companyFilter"*. Region filtering is done via `company_filter` with `regionId`. The vendor library still adds `region_filter` to the filter dict (tasks.py:223) but does not validate it against the unit allow-list — it is a no-op at runtime.

### Example Filter Expression

```
company_filter = "tvCompanyId IN (1,2,4,10) AND tvCompanyHoldingId IN (255)"
basedemo_filter = "sex = 2 AND age >= 25 AND age <= 55 AND incomeGroupRussia IN (2,3)"
```

> **Last verified: 2026-10-28 — sources: [url_2], [url_2b], [url_3], vendor_library v1.8.3 (checks.py:115-116)**

---

## Sortings

| Parameter | JSON Key | Type | Default | Supported By |
|---|---|---|---|---|
| `sortings` | `sorting` | `dict` | `null` | `timeband`, `simple`, `crosstab` |

**JSON shape:** `{sorting: {sortingUnits: [{unit: "<slice|statistic name>", direction: "ASC"|"DESC"}]}}`

**Dict input format:** `{column: 'ASC'|'DESC'}` — the library (`vendor_library core/tasks.py:559-573` and `vendor_library core/tasks.py:200-214`) wraps the input dict into the JSON shape above.

**Validation rules:**
- `unit` must be in `(statistics + slices)`.
- `direction` must be `ASC` or `DESC`.
- Validated in `checks.check_units_in_task`.

**Examples:**
```python
sortings = {'tvCompanyName': 'ASC'}
sortings = {'researchDate': 'ASC', 'RtgPer': 'DESC'}
```

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (core/tasks.py:200-214, core/tasks.py:559-573, checks.py)**

---

## Reach Conditions

| Parameter | JSON Key | Type | Default | Supported By |
|---|---|---|---|---|
| `reach_conditions` | `reachConditions` | `dict` | `null` | `timeband`, `simple`, `crosstab`, `consumption-target`, `duplication-timeband` |

**Subkeys:**

| Subkey | Type | Valid Values | Description |
|---|---|---|---|
| `filterType` | `str` | `MINUTES`, `PERCENT` | Condition measured in minutes or as a % of the interval. |
| `viewingType` | `str` | `NONCONSECUTIVE`, `CONSECUTIVE` | NONCONSECUTIVE = viewing with channel switches; CONSECUTIVE = viewing without switches. |
| `intervals` | `list` | `[{from, to}]` | One or more `{from, to}` intervals; bounds optional. Intervals must NOT overlap (overlapping intervals require separate tasks). |

**Example:**
```json
{"filterType": "MINUTES", "viewingType": "NONCONSECUTIVE", "intervals": [{"from": 5, "to": 9}, {"from": 10}]}
```

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3**

---

## Frequency Distribution Conditions

| Parameter | JSON Key | Type | Default | Supported By |
|---|---|---|---|---|
| `frequency_dist_conditions` | `frequencyDistConditions` | `dict` | `null` | `simple`, `crosstab` |

> ✅ **Note:** `frequency_dist_conditions` IS supported by `simple` per the vendor `filter_support_matrix_by_report` (`["simple", "crosstab"]`). Only `timeband` does not support it. The timeband template should not include this parameter.

**Subkeys:**

| Subkey | Type | Description |
|---|---|---|
| `intervals` | `list` of `{from, to}` | One or more contact-count intervals; bounds optional. Drives the `FrequencyDist000` / `FrequencyDistPer` statistics. |

**Example:**
```json
{"intervals": [{"from": 1}, {"from": 3}, {"from": 5}, {"from": 1, "to": 3}, {"from": 4, "to": 7}, {"from": 8, "to": 20}]}
```

**Template usage:** `[RF: last_category/nat_tv_brands_weekly_reach.json5]` sets 8 intervals (from=1 through from=20).

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (tasks.py)**

---

## Scales

| Parameter | JSON Key | Type | Default | Supported By |
|---|---|---|---|---|
| `scales` | `scales` | `dict` | `null` | All (5 documented types) ⚠️ **Conditional** |

> ⚠️ **Undocumented in vendor docs:** The only evidence for `scales` is `checks._check_scales` in the vendor library. Required when statistics include `drfd` or `reachN`.

**Format:** `{'<statistic_name>': [(from, to), ...]}`

**Scale statistics:** `drfd`, `reachN`

**Example:**
```json
{"drfd": [[0, 10], [11, 30]]}
```

> **Last verified: 2026-10-28 — sources: vendor_library v1.8.3 (checks.py:199-208, `scale_statistics = ['drfd', 'reachN']`)

---

## Custom Variable Identifiers

The 6 custom variable ID parameters select a custom respondent/demo/company/time/duplication variable for Consumption Target / respondent-level slicing. Retrieved via `cats.get_custom_respondent_variable()` (catalogs.py line 3626). NOT to be confused with the demographic attributes (e.g., `sex`, `age`) documented in [§6](#6-slices-dictionary).

| Parameter | JSON Key | Type | Supported By | Example UUID |
|---|---|---|---|---|
| `custom_demo_variable_id` | `customDemoVariableId` | `str` | consumption-target | — |
| `custom_company_variable_id` | `customCompanyVariableId` | `str` | consumption-target | — |
| `custom_respondent_variable_id` | `customRespondentVariableId` | `str` | consumption-target | `d2f99f6c-06ca-462e-b7dd-cce9d1037b1e` |
| `custom_time_variable_id` | `customTimeVariableId` | `str` | crosstab only | — |
| `custom_duplication_time_variable_id` | `customDuplicationTimeVariableId` | `str` | duplication-timeband | — |
| `custom_duplication_company_variable_id` | `customDuplicationCompanyVariableId` | `str` | duplication-timeband | — |

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (tasks.py:50-59, catalogs.py:3626)**

---

## City Dataset Features (kitId=3, TV_REG only)

| Parameter | JSON Key | Type | Default | Supported By |
|---|---|---|---|---|
| `add_city_to_basedemo_from_region` | `options/add_city_to_basedemo_from_region` | `bool` | `true` (TV_REG) | TV_REG only (kitId=3) |
| `add_city_to_targetdemo_from_region` | `options/add_city_to_targetdemo_from_region` | `bool` | `true` (TV_REG) | TV_REG only (kitId=3) |

**Behavior:** When True, `basedemo_filter` / `targetdemo_filter` are auto-populated with the matching city ID derived from the region in `company_filter` (library `build_task` lines 192-198). One city per task; multiple cities require separate tasks.

**Catalog method:** `cats.get_tv_monitoring_cities(region_name=[...])` — maps region ↔ city.

**Example:**
```python
# company_filter selects region 1 (Moscow)
company_filter = "regionId = 1"
# library auto-populates basedemo_filter with city derived from region
basedemo_filter = "city = 12"
```

**Template usage:** [TV-Reg] DATA_DEFAULTS set both to `true`. [TV] templates do not include these keys (TV uses kitId=1, not the Cities kit).

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (tasks.py:192-198)**

---

## Consumption Target Options

| Parameter | JSON Key | Type | Default | Supported By |
|---|---|---|---|---|
| `consumption_target_options` | `consumptionTargetOptions` | `dict` | `null` | consumption-target only |

**Subkeys:**

| Subkey | Type | Valid Values | Description |
|---|---|---|---|
| `groupsCount` | `str` (digit) | `"5"` (typical) | Number of groups to split selected respondents into; works with `NGroupResp` / `NGroupDur` statistics. |
| `intervalConditions` | `dict` | — | Condition object (see below). |

**`intervalConditions` subkeys:**

| Subkey | Type | Valid Values | Description |
|---|---|---|---|
| `intervals` | `list` of `{from, to}` | viewing-duration minutes | Duration intervals; bounds optional. |
| `viewingType` | `str` | `NONCONSECUTIVE`, `CONSECUTIVE` | Viewing type for the interval condition. |

**Example:**
```json
{"groupsCount": "5", "intervalConditions": {"intervals": [{"from": "5", "to": "100"}], "viewingType": "CONSECUTIVE"}}
```

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (tasks.py)**

---

## Project-Level Parameters

These parameters are **NOT TVI API parameters** — they are used by `mko_get_mediascope_data` report definition files and are not sent to the vendor API as task parameters. They are clearly marked 📋.

| Parameter | Type | Default | Report Files | Description |
|---|---|---|---|---|
| 📋 `media` | `MediaType` enum | required | All RF | `TV` or `TV_REG`; selects the template directory (`defaults/tv/` or `defaults/tv_reg/`). |
| 📋 `report_type` | `ReportType` enum | required | All RF | `crosstab`, `timeband`, or `simple`; maps to vendor `task_type`. |
| 📋 `report_subtype` | `ReportSubtype` enum | `default_value` (empty) | All RF + TV-CB subtype blocks | Selects the named block in the template file to overlay. Used by `services.py:157`. |
| 📋 `data_lang` | `str` (`'ru'` or `'en'`) | `'ru'` (models.py:193) | All RF | Output language; triggers `en_to_ru` slice translation (`utils.py:341-351`: replaces `EName` → `Name`). |
| 📋 `relative_path` | `Path` | required | All RF | Output directory for CSV exports. |
| 📋 `target_audiences` | `dict[str, str \| None]` | `{'not_set': null}` | All RF | Dict mapping audience label → demo filter expression; applied as `basedemo_filter` at task generation time. |
| 📋 `compression` | `dict[str, str] \| None` | `{'method': 'gzip'}` (models.py:190-192) | 13 of 17 RF | CSV output compression method. |
| 📋 `check_done` | `bool \| None` | `True` (models.py:186) | ReportSettings default | Skip existing output files. |
| 📋 `multiple_files` | `bool` | `True` (models.py:195) | All RF | If true and period > 1 year, split exports by month/week per `time_unit`. **Not a vendor API parameter** — it is a project wrapper around the TVI API's ~366-day per-task limit. |
| 📋 `period` / `last_time` | `PeriodModel` / `dict` | — | All RF | Either `date_filter` (list of date tuples) OR `last_time` + `time_unit`. Used to populate `date_filter` at runtime. |
| 📋 `task_type` | `str` | required | All RF | Vendor report type (`timeband`, `simple`, `crosstab`, `consumption-target`, `duplication-timeband`, `respondent-analysis`). Passed as the first positional argument to `build_task`. |
| 📋 `regions` | `list[int]` | absent | optional | TV_REG-only: restricts region iteration in `TVRegTaskStrategy`. If absent, iterates all available regions from `get_tv_region` catalog. Used by `reports.py:808`. |

> `en_to_ru` (`utils.py:341-351`) replaces `"EName"` with `"Name"` in slice names when `data_lang = "ru"`.

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3, project source (models.py, utils.py, services.py, reports.py)**

---

## Task Lifecycle

The TVI API task workflow proceeds through 5 stages. Each stage corresponds to a method on `MediaVortexTask` (and its project wrapper in `core/tasks.py`).

| Stage | Method | Input | Output | Role |
|---|---|---|---|---|
| 1 | `build_task` / `build_<type>_task` | Parameters (filters, slices, statistics, options) | JSON task spec | Assemble the task JSON from kwargs. |
| 2 | `send_task` / `send_<type>_task` | JSON task spec | Task ID | Submit the task to the API endpoint. |
| 3 | `wait_task` | Task ID | Task status | Poll for completion (handles retries, backoff). |
| 4 | `get_result` | Task ID | Result JSON | Retrieve the computed result. |
| 5 | `result2table` | Result JSON | `pandas.DataFrame` | Convert API result to a DataFrame. |

**Project-specific:** The vendor library's `TaskBuilder` class (in `mediascope_api/core/tasks.py`) provides `add_filter`, `add_slices`, `add_sortings` helper methods that transform snake_case kwargs into the JSON structure the API expects. The project's `core/tasks.py` manages task state via the `TVTask` class. `core/network.py` wraps `send_task` with retry logic (exponential backoff + jitter), a per-call deadline (`call_timeout`, default 300s), and concurrency limiting via `asyncio.Semaphore`.

**Supporting methods:** `get_active_tasks_count()`, `get_report_info()`, `get_excel_filename()`.

> Cross-references: `core/tasks.py`, `core/network.py`, vendor `mediavortex/tasks.py`

> **Last verified: 2026-10-28 — sources: [url_3], vendor_library v1.8.3 (core/tasks.py:200-214), project source (core/tasks.py TVTask class, core/network.py)**

---

## Validation Behavior

The vendor library enforces per-report parameter validity through two check layers:

### `check_task` (pre-submission)

- Validates types of each filter kwarg.
- For non-empty `str`/`dict` filters, extracts units (`sql.sql_to_units`) and verifies each unit name is in `task_types[task_type]['filters']` — kit-dependent, fetched at runtime via `cats.get_*_unit(kit_id)`.
- `region_filter` prints a deprecation notice: *"regionFilter в настоящее время не используется. Используйте companyFilter"* (checks.py:115-116).
- `date_filter` must be a non-empty list of `(start, end)` tuples.
- `sortings` must be a `dict`.
- `statistics` must be a non-empty `list`.

### `check_units_in_task` (post-assembly)

- Verifies each statistic in `tsk['statistics']` is in `task_types[task_type]['statistics']`.
- Verifies each filter unit in `tsk['filter']` is allowed.
- Verifies each slice in `tsk['slices']` is in `get_avl_slices(task_type)` (skipped for `consumption-target`).
- Verifies `sortings.sortingUnits` units are in `(statistics + slices)` and directions are `ASC`/`DESC`.

### Per-Kit Allow-Lists

The concrete allowed filters, statistics, and slices are **NOT hardcoded** — they are fetched at runtime by `cats.get_timeband_unit(kit_id)` through `get_respondent_analysis_unit(kit_id)`. These are stored in `tv_units[kit_id][unit_key] = {'statistics': [...], 'slices': [...], 'filters': [...]}`. `kitId=1` (TV Index All Russia) is the default used by docs examples.

> Cross-references: `vendor_library checks.py:199-208` (`_check_scales`), `vendor_library checks.py:154-162` (task_types dispatch), `vendor_library catalogs.py` (`get_*_unit` methods)

> **Last verified: 2026-10-28 — sources: vendor_library v1.8.3 (checks.py, catalogs.py)**

---

## Report Templates Cross-Reference

### Report Type → Template File → Subtypes

| Template File | Report Type | Has DATA_DEFAULTS | Subtype Blocks | Notes |
|---|---|---|---|---|
| `defaults/tv/timeband.json5` | timeband | Yes | None | Identical (byte-for-byte, 17 lines) to `tv/simple.json5` |
| `defaults/tv/simple.json5` | simple | Yes | None | Identical to `tv/timeband.json5` |
| `defaults/tv/crosstab.json5` | crosstab | Yes | 7 (`DYNAMICS_BY_SPOTS`, `DYNAMICS_BY_SPOTS_DICT`, `TOP_NAT_TV_ADVERTISERS`, `TOP_NAT_TV_PROGRAMS`, `NAT_TV_CHANNELS_ATV`, `NAT_TV_CHANNELS_BA`, `NAT_TV_CHANNELS_SOC_DEM`) | |
| `defaults/tv_reg/timeband.json5` | timeband | Yes | None | Identical (byte-for-byte, 32 lines) to `tv_reg/simple.json5` |
| `defaults/tv_reg/simple.json5` | simple | Yes | None | Identical to `tv_reg/timeband.json5` |
| `defaults/tv_reg/crosstab.json5` | crosstab | Yes | 2 (`DYNAMICS_BY_SPOTS`, `DYNAMICS_BY_SPOTS_DICT`) | Uses unquoted JSON5 keys (inconsistent with other templates) |

> ⚠️ **Template file sharing:** `tv/simple.json5` and `tv/timeband.json5` are byte-for-byte identical (both 17 lines). `tv_reg/simple.json5` and `tv_reg/timeband.json5` are identical (both 32 lines). This sharing causes filter-support mismatches: the TV shared file includes `time_filter` (invalid for simple per vendor), and the TV_REG shared file includes `program_filter`/`break_filter`/`ad_filter`/`frequency_dist_conditions` (invalid for timeband per vendor). See [§21](#21-content-gaps--undocumented-parameters).

### Report Files → Media → Report Type → Subtype → Parameters

| Report File | Media | Report Type | Subtype | Parameters Set/Override |
|---|---|---|---|---|
| `reports/test.json5` | TV | crosstab | `TOP_NAT_TV_ADVERTISERS` | Inherits from template; `ad_filter: null` |
| `reports/nat_tv_tvreport.json5` (7 entries) | TV | crosstab/timeband | `TOP_NAT_TV_ADVERTISERS`, `TOP_NAT_TV_PROGRAMS`, `NAT_TV_CHANNELS_ATV` (×2), `NAT_TV_CHANNELS_BA`, `NAT_TV_CHANNELS_SOC_DEM` (×2) | `company_filter` (6 of 7 entries), `break_filter`, `ad_filter: null`, `targetdemo_filter` (`'sex = 2'` in 1), `options.kitId` (1 or 7), `options.totalType` (`'TotalTVSet'`), `slices`, `statistics` |
| `reports/nat_tv_brands_last.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` | Inherits from template subtype |
| `reports/nat_tv_brands_tefal.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` + `DYNAMICS_BY_SPOTS_DICT` | Inherits from template; `data_lang: 'en'` in 1 entry |
| `reports/nat_tv_brands_theiss.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` (×3) + `DYNAMICS_BY_SPOTS_DICT` | Inherits from template |
| `reports/gap_resurs/nat_tv_brands.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` + `DYNAMICS_BY_SPOTS_DICT` | Inherits from template |
| `reports/gap_resurs/reg_tv_by_spots.json5` | TV_REG | crosstab | `DYNAMICS_BY_SPOTS` | `target_audiences` (TV_REG-specific: `all 4+`, `all 30+`, `all 55+ AB`, etc.) |
| `reports/last_category/nat_tv_brands_last.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` + `DYNAMICS_BY_SPOTS_DICT` | Inherits |
| `reports/last_category/nat_tv_brands_snack.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` + `DYNAMICS_BY_SPOTS_DICT` | Inherits |
| `reports/last_category/nat_tv_brands_weekly_reach.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` | `options` override (kitId, issueType, standRtg, baseDate, baseDateCalcType, oneBaseDatePerEachDateRange, useNbd), `frequency_dist_conditions` (8 intervals) |
| `reports/sovcombank/nat_tv_brands_finance.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` + `DYNAMICS_BY_SPOTS_DICT` | `compression: {method: 'gzip'}` |
| `reports/sovcombank/nat_tv_brands_finance2.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` + `DYNAMICS_BY_SPOTS_DICT` | `compression: {method: 'gzip'}` |
| `reports/sovcombank/nat_tv_by_spots.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` (×2) + `DYNAMICS_BY_SPOTS_DICT` | `compression: {method: 'gzip'}` |
| `reports/sovcombank/nat_tv_finance_q.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` | `options` (kitId=7), `slices` (week-based), `statistics` |
| `reports/sovcombank/reg_tv_by_spots.json5` | TV_REG | crosstab | `DYNAMICS_BY_SPOTS` | `target_audiences` (TV_REG-specific) |
| `reports/sovcombank/tv_msk_new_creatives.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` | `options.kitId=6` (TV Index Moscow) |
| `reports/sovcombank/tv_msk_new_creatives_short.json5` | TV | crosstab | `DYNAMICS_BY_SPOTS` | `options.kitId=6`, `multiple_files: false` |

### Template File Summary (by Media & Report Type)

| Media | Report Type | Subtypes Used in Report Files |
|---|---|---|
| TV | crosstab | `DYNAMICS_BY_SPOTS` (most common, 12+ report entries), `DYNAMICS_BY_SPOTS_DICT` (paired with above), `TOP_NAT_TV_ADVERTISERS` (test.json5, nat_tv_tvreport.json5), `TOP_NAT_TV_PROGRAMS` (nat_tv_tvreport.json5), `NAT_TV_CHANNELS_ATV` (nat_tv_tvreport.json5), `NAT_TV_CHANNELS_BA` (nat_tv_tvreport.json5), `NAT_TV_CHANNELS_SOC_DEM` (nat_tv_tvreport.json5, 2 entries as timeband) |
| TV | timeband | `NAT_TV_CHANNELS_SOC_DEM` (2 entries in nat_tv_tvreport.json5) |
| TV | simple | None (template exists but is unused) |
| TV_REG | crosstab | `DYNAMICS_BY_SPOTS` (gap_resurs, sovcombank/reg_tv), `DYNAMICS_BY_SPOTS_DICT` (gap_resurs) |
| TV_REG | timeband | None |
| TV_REG | simple | None |

### Kit ID Usage in Report Files

| kitId | Kit Name | Report Files |
|---|---|---|
| 1 (TV Index All Russia) | TV Index All Russia | nat_tv_tvreport.json5 (TOP_NAT_TV_ADVERTISERS, NAT_TV_CHANNELS_*) |
| 3 (TV Index Cities) | TV Index Cities | gap_resurs/reg_tv_by_spots.json5, sovcombank/reg_tv_by_spots.json5 |
| 6 (TV Index Moscow) | TV Index Moscow | sovcombank/tv_msk_new_creatives.json5, tv_msk_new_creatives_short.json5 |
| 7 (Big TV) | Big TV | last_category/nat_tv_brands_weekly_reach.json5, nat_tv_tvreport.json5 (NAT_TV_CHANNELS_BA entry) |

> **Template subtypes use kitId=7** for TV crosstab subtypes and **kitId=3** for TV_REG defaults.

> **Last verified: 2026-10-28 — sources: [url_3], project templates, research_templates.json**

---

## Content Gaps & Undocumented Parameters

### Vendor Parameters with No Description in Research

| Parameter | Gap | Evidence |
|---|---|---|
| `scales` | Undocumented in `_help.ipynb` and docs URLs; only evidence is `checks._check_scales` | `checks.py:199-208`: validates statistics against `cats.get_*_unit(kit_id)` at runtime; `scale_statistics = ['drfd', 'reachN']` |
| `custom_demo_variable_id` | Listed in `build_task` signature but no usage example in notebook | `vendor tasks.py` lines 50-59 |
| `custom_company_variable_id` | Same — no notebook example or docs page | Same |
| `custom_time_variable_id` | No notebook documentation; only mentioned as supported by crosstab | Line 220: "Filter set identical to Simple plus custom_time_variable_id" |
| `subject_filter` | Listed in `build_task` signature; not in notebook's documented filter list | `filter_support_matrix_by_report` does not include it for timeband |
| `duration_filter` | Listed in `build_task` signature; not in notebook's documented filter list | Supported by consumption-target, duplication-timeband only |
| `respondent_filter` | Listed in `build_task` signature; not in notebook's documented filter list | Supported by timeband, simple, crosstab |
| `platform_filter` | Listed in `build_task` signature; not in notebook's documented filter list | Supported by timeband, simple, crosstab |
| `playbacktype_filter` | Listed in `build_task` signature; not in notebook's documented filter list | Supported by timeband, simple, crosstab |
| `bigtv_filter` | Listed in `build_task` signature; not in notebook's documented filter list | Supported by timeband, simple, crosstab |
| `duplication_company_filter` | Listed in `build_task` signature; not in notebook | Supported by duplication-timeband only |
| `duplication_time_filter` | Listed in `build_task` signature; not in notebook | Supported by duplication-timeband only |
| `WAgeAverage` | Present in URL-1 Timeband list but absent from the notebook's Timeband cell | `research_vendor_2.json` discrepancies |
| Big TV (7-kit) statistics | Child page 4.1 "Статистики набора данных Big TV" was not fetched | URL-1 links to it but it was not one of the 3 assigned URLs |
| `respondent-analysis` report type | Filter/slice/statistics lists not documented anywhere | `vendor tasks.py task_urls`, `checks.py task_types`, `catalogs.py:4944`; not in notebook |

### Template Parameters Not in Vendor Docs (Project-Level)

| Parameter | Vendor Documented? | Template Usage | Notes |
|---|---|---|---|
| `data_lang` | No | All RF entries | Controls output language; triggers `en_to_ru` translation (`utils.py:341-351`) |
| `relative_path` | No | All RF entries | Output directory for CSV |
| `target_audiences` | No | All RF entries | Dict of audience label → filter expression; applied as `basedemo_filter` |
| `compression` | No | 13 of 17 RF files | CSV compression method; default `{method: 'gzip'}` |
| `check_done` | No | ReportSettings default | Skip existing files |
| `media` | No | All RF entries | `MediaType` enum (TV / TV_REG) |
| `report_type` | No (relates to vendor `task_type`) | All RF entries | `ReportType` enum |
| `report_subtype` | No | All RF entries + TV-CB subtypes | `ReportSubtype` enum |
| `regions` | No | TV_REG reports (optional) | `list[int]`; restricts region iteration in `TVRegTaskStrategy` (reports.py:808). If absent, iterates all regions. |

### Project-Level Classification Corrections

The following parameters are documented in the vendor docs but are **project-level** in this tool's report definition files — they are not standard `build_task` kwargs but wrapper-level options:

| Parameter | Vendor Documented As | Project-Level Classification | Notes |
|---|---|---|---|
| `multiple_files` | Listed among vendor "Other parameters" | 📋 Project-level (`models.py:195`) | If true and period > 1 year, split exports by month/week per `time_unit`. **Not a TVI API parameter** — it is a wrapper around the API's ~366-day per-task limit. |
| `time_unit` | N/A | 📋 Project-level (`options.period_split`) | Split frequency for multi-year periods. |
| `last_time` | N/A | 📋 Project-level | `{unit_count, include_current}` — "last N periods" convenience for `date_filter`. |

### Template Issues (from `research_templates.json`)

| # | Issue | Severity | Files Affected | Recommendation |
|---|---|---|---|---|
| TMPL-01 | `time_filter` present in TV simple template DATA_DEFAULTS (shared file) but vendor does not support `time_filter` for simple | Low (null is safe) | `tv/simple.json5`, `tv/timeband.json5` | Split the shared file; remove `time_filter` from simple. |
| TMPL-02 | `time_filter` absent from TV_REG timeband/simple templates | ⚠️ Medium | `tv_reg/timeband.json5`, `tv_reg/simple.json5` | Add `time_filter` to TV_REG timeband if time-of-day analysis is needed. |
| TMPL-03 | `program_filter` / `break_filter` / `ad_filter` / `frequency_dist_conditions` present in TV_REG timeband DATA_DEFAULTS but vendor does NOT support these for timeband. ⚠️ `frequency_dist_conditions` IS supported by Simple per vendor (`["simple", "crosstab"]`) — only timeband should lack it. | High | `tv_reg/timeband.json5`, `tv_reg/simple.json5` | Split the shared file; remove unsupported params from timeband. In simple, retain `frequency_dist_conditions` (vendor supports it). |
| TMPL-04 | Duplicate slices in `tv/crosstab.json5`: `articleList4EName` appears twice in DYNAMICS_BY_SPOTS; `researchMonth` appears twice in TOP_NAT_TV_PROGRAMS | Medium | `tv/crosstab.json5` | Remove duplicates. |
| TMPL-05 | Duplicate `tvNetId` values in `nat_tv_channels_ATV/BA/SOC_DEM` subtypes and `nat_tv_tvreport.json5` (`348,348,348`) | Low | `tv/crosstab.json5`, `nat_tv_tvreport.json5` | Verify if intentional; remove if accidental. |
| TMPL-06 | `issueType="AD"` set in TV_REG timeband/simple DATA_DEFAULTS but vendor says `issueType` is Crosstab-specific | Medium | `tv_reg/timeband.json5`, `tv_reg/simple.json5` | Move `issueType` out of non-crosstab DATA_DEFAULTS. |
| TMPL-07 | `tv_reg/crosstab.json5` uses unquoted JSON5 keys while other templates use quoted keys | Low | `tv_reg/crosstab.json5` | Standardize to quoted keys for consistency. |

### Vendor Doc Pages 404 / Unreachable

| Page | URL | Status | Impact |
|---|---|---|---|
| Crosstab filter/slice page | `/Отчет Кросс-таблица/` | 404 NOT FOUND | Crosstab filter/slice lists only obtainable from notebook + library. Filter set verified = Simple filter set. |
| Consumption Target filter/slice page | `/Отчет ConsumptionTarget/` | 404 NOT FOUND | Parameters obtainable from notebook + library only. |
| Duplication Timeband filter/slice page | `/Отчет Пересечение аудитории/` | 404 NOT FOUND | Parameters obtainable from notebook + library only. |
| `pageId=78807053` ("справка по параметрам") | `https://docs.mediascope.net/pages/viewpage.action?pageId=78807053` | Empty wiki section, no body content | The notebook's promised detailed attribute reference is not retrievable. |

### Vendor Parameter No-Ops

| Parameter | Status | Evidence |
|---|---|---|
| `region_filter` | ⚠️ DEPRECATED — no-op at runtime | `checks.check_task:115-116` prints deprecation notice; vendor library adds it to filter dict but does not validate against unit allow-list. Use `company_filter` with `regionId` instead. |

> **Last verified: 2026-10-28 — sources: [url_1], [url_2], [url_2b], [url_3], vendor_library v1.8.3, project templates, research_templates.json**

---

## Metadata

### Research Sources

| ID | URL / Path | Title | Status | Language |
|---|---|---|---|---|
| `url_1` | `https://docs.mediascope.net/xwiki/bin/view/Main/TV%20Index%20API/TVI%20API:%20%D1%81%D1%82%D0%B0%D1%82%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8/` | 4. TVI API: статистики | Fetched | ru |
| `url_2` | `https://docs.mediascope.net/xwiki/bin/view/Main/TV%20Index%20API/TVI%20API:%20%D1%84%D0%B8%D0%BB%D1%8C%D1%82%D1%80%D1%8B%20%D0%B8%20%D1%81%D1%80%D0%B5%D0%B7%D1%8B/%D0%9E%D1%82%D1%87%D0%B5%D1%82%20Timeband/` | 3.1. Отчет Timeband (filter structure + slices) | Fetched | ru |
| `url_2b` | `https://docs.mediascope.net/xwiki/bin/view/Main/TV%20Index%20API/TVI%20API:%20%D1%84%D0%B8%D0%BB%D1%8C%D1%82%D1%80%D1%8B%20%D0%B8%20%D1%81%D1%80%D0%B5%D0%B7%D1%8B/%D0%9E%D1%82%D1%87%D0%B5%D1%82%20Simple/` | 3.2. Отчет Simple (filter structure + slices) | Fetched | ru |
| `url_3` | `https://github.com/MEDIASCOPE-JSC/mediascope-jupyter/blob/main/tv_index/_help.ipynb` | _help.ipynb — official Mediascope Jupyter help notebook | Fetched (raw) | ru |
| `page_id` | `https://docs.mediascope.net/pages/viewpage.action?pageId=78807053` | справка по параметрам | **404/empty** | — |
| `vendor_library` | `.venv/Lib/site-packages/mediascope_api (mediascope-api-lib 1.8.3)` | Vendor library source | Read locally | ru (code comments) |
| `project_templates` | `src/mko_get_mediascope_data/settings/defaults/` + `settings/reports/` | Project JSON5 templates and report files | Read locally | en |

### Vendor Library Files Read

- `mediavortex/tasks.py` — `build_task` signature, `build_*_task` convenience methods, JSON key construction, `add_sortings`/`add_filter`/`add_slices` transformation methods
- `mediavortex/checks.py` — `check_task`, `_check_filter_units`, `check_units_in_task`, `_check_scales`, `get_avl_slices`, `get_avl_stats`
- `mediavortex/catalogs.py` — `get_*_unit` methods, `get_respondent_analysis_unit`, `get_tv_monitoring_cities`, `get_tv_demo_attribute`, `get_tv_company`, `get_tv_net`, `get_tv_region`, `get_tv_location`, `get_tv_thematic`

### Verification of 3 Key Claims (from `research_vendor_1.json`)

| Claim | Verification | Result |
|---|---|---|
| `time_filter` support matrix | `build_*_task` signatures: timeband ✅ (tasks.py:326), simple ✗ (tasks.py:443), crosstab ✗ (tasks.py:575), consumption-target ✅ (tasks.py:708), duplication-timeband ✅ (tasks.py:829) | CONFIRMED |
| Statistics completeness | `checks.py:199-208` — `_check_scales` validates at runtime via `cats.get_*_unit(kit_id)`; `RtgPerSum` (Crosstab), `ATV` (Simple), `Reach000` (Timeband) all in URL-1 | CONFIRMED |
| Sortings JSON shape | `vendor_library core/tasks.py:200-214` and `vendor_library core/tasks.py:559-573` — sortings dict → `{sorting: {sortingUnits: [{unit, direction}]}}` | CONFIRMED |

### Counts Summary

| Metric | Count |
|---|---|
| Total template files | 6 |
| Total report definition files | 17 |
| TV crosstab subtype blocks | 7 |
| TV_REG crosstab subtype blocks | 2 |
| TV timeband/simple subtype blocks | 0 |
| TV_REG timeband/simple subtype blocks | 0 |
| Statistics — Timeband | 29 |
| Statistics — Simple | 53 |
| Statistics — Crosstab | 74 (39 Sum + 35 Avg) |
| Statistics — Consumption Target | 4 |
| Statistics — Duplication Timeband | 4 |
| Statistics — Respondent Analysis | Undocumented |

### Maintenance Process

| Step | Action | Cadence |
|---|---|---|
| 1 | Re-fetch vendor docs URLs (3 pages) | Before each major version change |
| 2 | Re-read vendor library source (`tasks.py`, `checks.py`, `catalogs.py`) | Quarterly or after library upgrade |
| 3 | Cross-check against project template files | After any template modification |
| 4 | Update "Last Verified" timestamp in affected sections | Every update |
| 5 | Verify against `research_vendor_1.json` peer findings | After parallel investigations |
| 6 | Run template verification checklist (§21 issues TMPL-01 through TMPL-07) | In pre-commit or CI |

> **Last verified: 2026-10-28 — sources: [url_1], [url_2], [url_2b], [url_3], vendor_library v1.8.3**

---

## Appendix A: Report Type Enum Mapping (Project vs Vendor)

| Project `ReportType` | Vendor `task_type` | Endpoint | Library Builder |
|---|---|---|---|
| `crosstab` | `crosstab` | `/task/crosstab` | `build_crosstab_task` |
| `timeband` | `timeband` | `/task/timeband` | `build_timeband_task` |
| `simple` | `simple` | `/task/simple` | `build_simple_task` |
| *(not supported)* | `consumption-target` | `/task/consumption-target` | `build_consumption_target_task` |
| *(not supported)* | `duplication-timeband` | `/task/duplication-timeband` | `build_duplication_timeband_task` |
| *(not supported)* | `respondent-analysis` | `/task/respondent-analysis` | *(no builder method)* |

## Appendix B: Subtype Enum Reference

| Subtype | TV Template | TV_REG Template | Purpose |
|---|---|---|---|
| `DYNAMICS_BY_SPOTS` | ✅ | ✅ | Ad dynamics by product/advertiser/brand |
| `DYNAMICS_BY_SPOTS_DICT` | ✅ | ✅ | Dictionary reference export |
| `TOP_NAT_TV_ADVERTISERS` | ✅ | — | Top national TV advertisers |
| `TOP_NAT_TV_PROGRAMS` | ✅ | — | Top national TV programs |
| `NAT_TV_CHANNELS_ATV` | ✅ | — | National TV channels — ATV metrics |
| `NAT_TV_CHANNELS_BA` | ✅ | — | National TV channels — Billing Audience |
| `NAT_TV_CHANNELS_SOC_DEM` | ✅ | — | National TV channels — Socio-demographics |

> **Note:** TV_REG crosstab has only 2 subtypes (`DYNAMICS_BY_SPOTS`, `DYNAMICS_BY_SPOTS_DICT`). TV and TV_REG simple/timeband templates are identical and have no subtype blocks.

---

*End of reference. All counts and citations verified against `research_vendor_2.json`, `research_vendor_1.json`, `research_templates.json`, `validation_report.md`, vendor library v1.8.3, and project source code as of 2026-10-28.*
