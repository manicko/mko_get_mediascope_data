# Step 3 Validator Decision — Audit Phases Coverage Review

## 1. Evidence Base

### 1.1 Existing content phases (author files present)
| Phase | Theme | Owned concerns (summary) |
|-------|-------|--------------------------|
| 01 | Entry Points & Process Architecture | Entry surfaces, import-time behaviour, singleton bootstrap, config bootstrap gate, stdout reconfiguration, import-time `dictConfig` side effects |
| 02 | Configuration & Secrets Management | Credential sourcing/ingress, Pydantic validation & defaults merge, fail-fast guards, dead config, env separation |
| 03 | Async Concurrency & Worker Safety | Semaphore ceiling, worker cancellation/shutdown, Queue backpressure (maxsize=100), error isolation, retry-isolation within concurrency boundary |
| 04 | Data Pipeline & Report Lifecycle | Period resolution & API clamping, task generation (Strategy), send→wait→extract ordering, DataFrame conversion, CSV export (`mode="x"`, silent skip), error serialisation (`TVTask.to_yaml`) |
| 05 | Config Template Validation & Spec Conformance | Template-to-Pydantic model conformance, field-name drift, `extra` policy, declared-spec-vs-what-code-reads |
| 09 | External Integrations & API | Credential seam on the wire, retry contract, backoff+jitter, blocking-work bridge (`to_thread`), degradation blast radius, what API client writes to logs, declared-vs-code dependency claims |
| 10 | Code Quality | Fixed-value homes, constant-surface maintenance, boundary validation, type-width & suppressions, single-responsibility per unit, one-rule-one-home, convention compliance, declared contracts & enforcement, template/config drift |
| 11 | Test Coverage | Exercised-not-reachable, doubles remove properties, fixture data contract, both-sides-of-boundaries, harness as system, reproducibility, merge-blocking gates, declared test-layer contracts |
| 12 | Production Ops, Security & Observability | Controls-that-examine-nothing, CI/CD gates, gate-vs-report, packaging/artifact identity, observability signals, runbooks, release & rollback |
| 13 | Performance & Scalability | Concurrency ceilings, per-task API call cost, retry backoff cost, worker saturation, memory pressure, observable latency |

### 1.2 Cross-references in existing phase files (verified via grep)

Phases **15, 16, 17** are explicitly referenced as deferred/out-of-scope responsibilities:

- **Phase 10, line 18**: "posture (12); measurable performance consequences (13); **authorization (15)**."
- **Phase 10, lines 19–20**: "Two live implementations of one derived quantity... are this phase's HIGH and LOW bands; **17** establishes the arithmetic showing whether they agree... A declared rendered-surface or accessibility contract measured against the document is **16's** measurement."
- **Phase 11, line 20**: "What the rendered outcome is, and what a derived quantity equals, are **16's** and **17's**."
- **Phase 99, line 16**: "the **seventeen content phases** audit the system."

Phases **06, 07, 08, 14** appear with **zero references** in any phase file (grep-verified) — their intended scope is undeclared and cannot be assessed.

### 1.3 Key source-code facts (verified against implementation)

| Concern | Location | Verdict |
|---------|----------|---------|
| YAML deserialization | `utils.py:355` — `yaml.safe_load(cfg)` | **Already safe.** No `yaml.load`/`unsafe_load` found anywhere. |
| Authorization error handling | `network.py:10` imports `AuthorizationError`; `:80–86` catches 401 and re-raises (non-retriable) | Authorization flow is a real, distinct code path. |
| `refresh_token_func` TODO | `network.py:46` — commented as "TODO (Architect)" | Token refresh not yet implemented; authorization lifecycle incomplete. |
| `print()` in core modules | `reports.py:265`; `services.py:117,124,154` | Violates the "no `print()` in `core/`" standard; within Phase 10's scope. |
| CSV export mode | `utils.py:192` — `mode="x"` | Silent skip on existing file; already flagged by Phase 04 as CRITICAL. |
| Error-file target path | `tasks.py:32–33` — `Path().absolute() / "data" / "output" / ...` | Hardcoded to working directory, NOT the configured export folder; already flagged by Phase 04. |
| Import-time singleton + dictConfig | `services.py:163–166` | `app_service` and `dictConfig` execute at module import; within Phase 01. |
| stdout reconfiguration | `app.py:17` — `sys.stdout.reconfigure(...)` | Unconditional; affects host processes (Jupyter/Airflow); within Phase 01. |
| Trace IDs / structured logging | Searched: no matches in `src/` | No structured logging or correlation IDs. Observability partially covered by Phase 12 (block 5) and Phase 13 (block 6). |
| TLS configuration | No `ssl`/`verify`/`cert` references in `src/` | TLS handled by `requests` (via `mediascope-api-lib`); default verification on. |
| Worker pool size | `reports.py:92` — `max_tasks: int = 100` | 100 workers; within Phase 03's scope. |
| Dependency pinning | `pyproject.toml:15–29` — no lock file pinned versions | Packaging/identity within Phase 12's scope. |

---

## 2. Final Phase Decisions

### ✅ Phase 15 — Authorization & Access Control → **APPROVE**

**Proposed by:** Researcher 1 (Phase 15), Researcher 3 (Authorization & Credential Flow Management).

**Distinct responsibility boundary?** Yes. Phase 10 line 18 explicitly defers "authorization (15)" as a distinct scope boundary: *"This phase owns convention and consistency inside the code... not what a lifecycle decision should be, what a request is allowed to do."* Authorization is about policy — what is allowed, scope validation, auth-failure classification, credential/token lifecycle — not code conventions.

**No existing phase adequately covers it?** Confirmed:
- **Phase 02** covers credential *sourcing, validation, and protection* (file ingress, fail-fast guards) — not authorization policy or token lifecycle.
- **Phase 09** covers the outbound API *mechanics* (credential on the wire, retry contract) — not auth-failure classification or scope validation.
- **Phase 10** explicitly excludes authorization from its scope and defers it to Phase 15.

**Evidence from source:** `network.py` imports and handles `AuthorizationError` as a distinct, non-retriable 401 path (lines 80–86), with a TODO for `refresh_token_func` (line 46). The error handler logs `args`/`kwargs` that could expose credentials in logs (line 83–84), which is an authorization-context logging concern distinct from Phase 02's credential-protection focus.

**Action:** APPROVE. Researcher 3's *"Authorization & Credential Flow Management"* is **merged** into Phase 15 (same domain: token acquisition, refresh, auth-failure classification, scope validation, credential rotation). Next phase number: **15** (matches existing Phase 10 line 18 reference).

---

### ✅ Phase 16 — Rendered-Surface & Output Conformance Measurement → **APPROVE**

**Proposed by:** Researcher 1 (Phase 16).

**Distinct responsibility boundary?** Yes. Phase 10 line 20 establishes the distinction explicitly: *"A declared rendered-surface or accessibility contract **measured against the document** is 16's measurement; a contract that merely has no enforcement point is this phase's."* Phase 11 line 20 defers rendered-outcome conformance to Phase 16.

**No existing phase adequately covers it?** Confirmed:
- **Phase 04** covers the *mechanics* of export (file mode, filename pattern, path resolution, error-file location) — not conformance measurement of the output against a declared contract.
- **Phase 05** covers template-to-model conformance — not rendered-output-to-spec conformance.
- **Phase 10** covers whether a contract *exists* and has an *enforcement point* — Phase 16 measures whether the *actual rendered output conforms* to a declared surface.

**Evidence from source:** The `csv_to_file` function writes CSVs with locale-specific formatting (`decimal=","`, `sep=";"`, `encoding="utf-8-sig"`) and `mode="x"` (utils.py:184–200). The rendered CSV's schema and format are not verified against any declared output contract. Phase 16 would measure whether the actual output matches a declared rendered-surface spec.

**Action:** APPROVE. Next phase number: **16** (matches existing Phase 10 line 20 and Phase 11 line 20 references).

---

### ✅ Phase 17 — Arithmetic & Derived-Quantity Verification → **APPROVE**

**Proposed by:** Researcher 1 (Phase 17), Researcher 3 (Data Correctness & Numerical Integrity).

**Distinct responsibility boundary?** Yes. Phase 10 line 19-20: *"Two live implementations of one derived quantity... are this phase's HIGH and LOW bands; **17 establishes the arithmetic showing whether they agree** and records the duplication itself as a deferral here."* Phase 11 line 20: *"what a derived quantity equals... are 16's and 17's."*

**No existing phase adequately covers it?** Confirmed:
- **Phase 10** *detects* two implementations of a derived quantity (HIGH/LOW band) and explicitly *defers* the arithmetic comparison to Phase 17: *"records the duplication itself as a deferral here."*
- **Phase 04** handles DataFrame conversion and column matching — not numerical correctness of aggregations or derived quantities.
- **Phase 05** validates template-to-model field names — not arithmetic semantics.

**Evidence from source:** `utils.py:pivot_df_frequency` (lines 214–252) performs frequency-distribution pivoting with `aggfunc="first"` and interval-based column sorting — a place where aggregation semantics and floating-point fidelity matter. The `csv_to_file` function uses `decimal=","` and `sep=";"` (utils.py:193–194), which affects how numerical values are rendered and could cause silent data interpretation errors if the downstream consumer expects different conventions.

**Action:** APPROVE. Researcher 3's *"Data Correctness & Numerical Integrity"* (aggregation semantics, floating-point error bounds, null/NaN handling) is **merged** into Phase 17. Researcher 3's additional sub-concern of *"output CSV schema verification"* is redirected to **Phase 16** (rendered-surface conformance), since schema verification is an output-conformance measurement, not arithmetic verification. Next phase number: **17** (matches existing Phase 10 line 19 and Phase 11 line 20 references).

---

### ❌ Phase — Security (broad) → **REJECT**

**Proposed by:** Researcher 2 (Security), Researcher 4 (Security & Supply Chain).

**Why rejected:** A broad "Security" phase would **duplicate** existing coverage and create ownership ambiguity with Phase 12, which is already titled *"Production Operations, **Security** & Observability"* and covers CI/CD controls, packaging, and operational posture. Specific security sub-domains are already distributed:

| Security sub-concern | Existing phase owner | Evidence |
|---|---|---|
| Credential sourcing/protection | Phase 02 (blocks 2, 4, 7) | `network.py:80–86` logs `AuthorizationError` with args/kwargs; Phase 02 block 2 covers credential seam |
| API boundary / credential on wire | Phase 09 (block 1) | Explicitly "owns only outbound integration mechanics" |
| YAML deserialization safety | Already addressed; Phase 10 convention | `yaml.safe_load` at utils.py:355; Phase 10 block 7 covers convention compliance |
| Path traversal | Phase 10 (block 3: boundary validation) | `resolve_path` in utils.py:307–340; Phase 10 block 3 audits boundary validation |
| TLS / transport security | Phase 09 (external API boundary) | Phase 09 owns "every boundary between this system and the Mediascope API" |
| Dependency CVE scanning | Phase 12 (block 1: controls; block 2: CI/CD) | Phase 12 audits "every automated control that emits a pass/fail signal" |
| Data egress protection | Phase 04 (CSV export) + Phase 09 (dependency blast radius) | Phase 04 block 5 covers export safety; Phase 09 block 5 covers dependency blast radius |

The only genuinely unclaimed security sub-domain is **threat modeling** — a holistic activity that does not fit any single phase's checklist format and is marginal for a standalone CLI tool (no web server, no multi-tenant auth, no user RBAC). This is an extension recommendation, not a new phase.

**Action:** REJECT. Consolidate Researcher 2 and Researcher 4's security proposals. The unclaimed sub-concerns (threat modeling, data egress) are directed to the extension recommendations.

---

### ❌ Phase — Logging & Observability → **REJECT**

**Proposed by:** Researcher 2.

**Why rejected:** Observability is already distributed across three existing phases with explicit blocks:

| Concern | Existing phase | Block |
|---|---|---|
| Log file handles, failure-state counters, logging config | Phase 12 | Block 5: "Observability: signals produced and consumed" |
| What API client writes to logs & sanitisation | Phase 09 | Block 7: "What the integration surface writes into logs" |
| Latency measurement, slow-vs-stuck task attribution | Phase 13 | Block 6: "Observable latency and its source" |
| Import-time logging side effects | Phase 01 | Block 2: "Singleton Bootstrap at Import Time" |

The specific gap raised by Researcher 2 — *"structured logging with trace IDs for async correlation, error traceability through 100-worker pool"* — is a refinement of Phase 12 block 5 and Phase 13 block 6, not a distinct responsibility boundary that violates any existing phase's scope. Creating a standalone Observability phase would split what Phase 12 already claims ("Security & **Observability**") and would overlap with Phase 13's latency measurement.

**Action:** REJECT. The trace-ID gap is directed to extension recommendations for Phase 12 and Phase 13.

---

### ❌ Phase — Dependency & Build Reproducibility → **REJECT (conditional, not met)**

**Proposed by:** Researcher 4 (conditional).

**Why rejected:** Phase 12 already covers this domain comprehensively:

| Dependency/build concern | Phase 12 block | Evidence |
|---|---|---|
| Packaging, pinned dependency versions, artifact identity | Block 4: "Packaging & artifact identity" | `pyproject.toml` build metadata, setuptools backend, pinned deps (lines 1–29) |
| CI/CD gates, what checks gate a release | Block 2: "CI/CD pipeline & gates"; Block 1: "A control that runs, reports clean, and examines nothing" | Controls inventory — linter, type checker, tests, secret scan |
| Release & rollback, lock file, config schema compat | Block 7: "Release & rollback" | Version pinning, config backward-compatibility |

The only refinement opportunity is *explicitly* adding dependency-CVE-scan verification to Phase 12 block 1's control inventory. This is an extension, not a new phase.

**Action:** REJECT. Directed to extension recommendations for Phase 12.

---

## 3. Minimally Necessary New Phases — Final List

| Number | Responsibility (one line) | Source researcher(s) |
|--------|--------------------------|---------------------|
| **15** | Authorization policy, auth-failure classification, scope validation, and credential/token lifecycle (acquisition, refresh, rotation) | R1, R3 (merged) |
| **16** | Measurement of rendered output (CSV files, error files) against a declared surface/output contract | R1 |
| **17** | Arithmetic correctness of derived quantities: aggregation semantics, floating-point error bounds, null/NaN handling, multi-implementation agreement | R1, R3 (merged) |

**Phase 14 is left unassigned.** It has zero references in any phase file, and no researcher proposed a responsibility for it. The framework's "seventeen content phases" (Phase 99 line 16) implies 7 missing phases beyond the current 10; of these, 3 (15, 16, 17) are now defined. The remaining 4 (06, 07, 08, 14) are undeclared gaps with no proposed purpose — they remain reserved for future architectural decisions, consistent with Researcher 1's finding that they "cannot be assessed."

After this review, the content phase count is **13** (01–05, 09–13, 15, 16, 17). The unassigned gaps are 06, 07, 08, 14 — four phases still to be defined when architectural scope makes their purpose clear.

---

## 4. Extension Recommendations (Phase-Refinement Track)

These are **not** new phases. They refine existing phase blocks to cover sub-concerns that researchers raised but that do not warrant standalone phases. These belong to the `audit-phase-refine` track (audit-phase-refine.md), not to the Step 4 create-new-phase flow.

### Phase 01 — Entry Architecture
- **Extension:** Explicitly cover the import-time initialization lifecycle — the singleton `app_service` (services.py:163) and `logging.config.dictConfig` (services.py:166) executing at import, and the unconditional `sys.stdout.reconfigure` (app.py:17).
- **Source:** Researcher 3, Researcher 4.

### Phase 02 — Configuration & Secrets
- **Extension:** Enumerate credential lifecycle scenarios (rotation, expiry handling) and secret redaction in serialised error files (`TVTask.to_yaml` writes raw `settings` to YAML at tasks.py:36–52, which could contain sensitive filter values).
- **Source:** Researcher 4.

### Phase 03 — Async Concurrency
- **Extension:** Strengthen semaphore/queue bound correctness — verify that `max_tasks=100` workers (reports.py:92) and the `asyncio.Queue(maxsize=100)` (reports.py:115) interact correctly, and that the bare `t.cancel()` at reports.py:162 is awaited.
- **Source:** Researcher 4.

### Phase 04 — Data Pipeline
- **Extension:** Strengthen output contract stability (CSV schema verification) and error-file path correctness — `TVTask.to_yaml` writes to a hardcoded `Path().absolute() / "data" / "output" / ...` (tasks.py:32–33) instead of the configured export folder.
- **Source:** Researcher 4.

### Phase 09 — External API
- **Extension:** Add TLS/transport-security verification to the external API boundary audit, and strengthen log-content safety — the `AuthorizationError` handler logs `args`/`kwargs` (network.py:83–84) which could expose credential material.
- **Source:** Researcher 4, Researcher 2.

### Phase 10 — Code Quality
- **Extension:** Add YAML safety verification (confirm `yaml.safe_load` is used everywhere, not `yaml.load`) to the convention compliance block; flag `print()` calls in core modules (`reports.py:265`, `services.py:117,124,154`) as violations of the "no `print()` in core/" standard; flag Russian-language comments/log messages throughout core modules as violations of the "English Only" standard.
- **Source:** Researcher 4, Researcher 2.

### Phase 12 — Production Ops, Security & Observability
- **Extension:** Add dependency CVE scanning to the control inventory (block 1); add structured-logging/trace-ID verification to the observability block (block 5); add failed-task YAML error-file design and secret-redaction review (block 5 or block 1).
- **Source:** Researcher 4, Researcher 2.

### Phase 13 — Performance
- **Extension:** Add cross-task error traceability through the 100-worker pool as a sub-check in the latency measurement block (block 6).
- **Source:** Researcher 2.

---

## 5. Justification Summary

### Why this minimal set (3 new phases)?

The audit framework's own design — as recorded in the existing phase files — explicitly anticipates exactly three missing phases:

1. **Phase 15 (Authorization)** — Phase 10 line 18 explicitly defers "authorization (15)" as a scope boundary it does not own.
2. **Phase 16 (Rendered-Surface Measurement)** — Phase 10 line 20 and Phase 11 line 20 explicitly defer "rendered-surface measurement" to Phase 16.
3. **Phase 17 (Arithmetic Verification)** — Phase 10 line 19 and Phase 11 line 20 explicitly defer "arithmetic showing whether they agree" to Phase 17.

These three phases were not proposed speculatively — they were **designed into the framework** by the phase authors, then left unauthored. The phase files literally say "this is Phase 15's/16's/17's concern" and the files do not yet exist. This is the strongest possible evidence that they are necessary: the existing phases' scope boundaries explicitly carve them out.

### Why each is distinct:

| New phase | Distinct from | Why not folded into existing |
|-----------|--------------|------------------------------|
| Phase 15 (Authorization) | Phase 02 (credentials), Phase 09 (API mechanics), Phase 10 (code conventions) | Phase 10 *explicitly excludes* authorization; Phase 02 covers credential *protection* not *policy*; Phase 09 covers API *mechanics* not auth *decisions* |
| Phase 16 (Rendered-Surface) | Phase 04 (export mechanics), Phase 05 (template conformance) | Phase 04 covers *how* output is written; Phase 16 covers *whether* output conforms to a declared contract; Phase 05 covers config→model, not output→spec |
| Phase 17 (Arithmetic) | Phase 10 (detects duplication), Phase 04 (pipeline) | Phase 10 *detects* two implementations and *defers* the arithmetic to Phase 17; Phase 04 handles DataFrame columns, not numerical correctness |

### Why the other proposals were NOT approved:

- **Security (broad):** Phase 12 already bears the "Security" title and covers operational controls; Phase 02 covers credentials; Phase 09 covers the API boundary; Phase 10 covers code-level conventions. A standalone Security phase would duplicate these and create ownership conflicts. The code also already uses `yaml.safe_load` (no deserialization vulnerability to audit).

- **Logging & Observability:** Already distributed across Phase 12 (block 5), Phase 09 (block 7), and Phase 13 (block 6) with explicit, non-overlapping blocks. Trace-ID gaps are an extension to Phase 12/13.

- **Dependency & Build Reproducibility:** Fully covered by Phase 12's packaging (block 4), CI/CD (block 2), and release/rollback (block 7) blocks.

### Phase numbering

Phases 15, 16, 17 are assigned their **referenced numbers** — not the "next available" number 14. The existing phase files reference these specific numbers (Phase 10: "authorization (15)", "17 establishes the arithmetic", "16's measurement"; Phase 11: "16's and 17's"). Assigning any other number would break the cross-reference chain already embedded in the framework. Phase 14 remains an unassigned gap with no proposed purpose; it is left empty until an architectural scope emerges that justifies it (Researcher 1 confirmed it "cannot be assessed").

---

## 6. Rollout Analysis

| Phase | Dependencies | Risks | Sequencing |
|-------|-------------|-------|------------|
| 15 (Authorization) | None — can be created independently | Low risk; content phase only | Create after Step 3 approval; Step 4 doc-specialist |
| 16 (Rendered-Surface) | None — can be created independently | Low risk | Create after Step 3 approval; Step 4 doc-specialist |
| 17 (Arithmetic) | None — can be created independently | Low risk | Create after Step 3 approval; Step 4 doc-specialist |

**No code changes** are required or recommended at this step. Phase files are documentation artifacts (`.ai/audit/phases/NN-*.md`); creating new phase files does not touch source code, tests, or configurations. All extensions (Section 4) belong to the separate `audit-phase-refine` track.

**Numbering integrity verified:** The new phase files will use the exact numbers (15, 16, 17) already cross-referenced by Phase 10 (line 18: "(15)"; lines 19–20: "(17)" and "16's") and Phase 11 (line 20: "16's and 17's"). Phase 99 (validator) remains reserved and is not a content phase.

---

## 7. Execution Validation

| Criterion | Status | Evidence |
|-----------|--------|----------|
| Phase targets still exist (file paths not referenced) | ✅ N/A — creating new files | Phases 15, 16, 17 have no existing files to conflict with |
| Plan is not stale | ✅ | Phase files read 2026-10-08; cross-references verified via grep |
| Architecture remains consistent | ✅ | No code changes; new phases align with existing framework design |
| Task is applicable | ✅ | Step 3 of audit-phase-gaps.md explicitly delegates to the validator |

---

## 8. Warnings

| Risk | Severity | Mitigation |
|------|----------|------------|
| Phase 14 remains unassigned — framework says "seventeen content phases" but only 13 will exist after these 3 are created | Medium | Leave 14 as a reserved gap; document that 06/07/08/14 are undeclared and require future architectural scoping before assignment |
| Security concerns are distributed, not centralized — risk of oversight | Medium | The extension recommendations (Section 4) direct specific security sub-concerns to existing phases; ensure the phase-refinement track addresses them |
| Researcher count of "existing 11 phases" is off by one (there are 10 content + 1 validator = 11 total) | Low | Cosmetic; does not affect decisions |

---

## Decision Record

| Proposal | Researcher(s) | Decision | Merged into | Phase number |
|----------|----------------|----------|-------------|--------------|
| Phase 15 — Authorization & Access Control | R1, R3 | **APPROVE** | R3's "Authorization & Credential Flow Management" | 15 |
| Phase 16 — Rendered-Surface & Output Conformance Measurement | R1 | **APPROVE** | — | 16 |
| Phase 17 — Arithmetic & Derived-Quantity Verification | R1, R3 | **APPROVE** | R3's "Data Correctness & Numerical Integrity" (arithmetic sub-concerns; CSV schema → Phase 16) | 17 |
| Security (broad) | R2, R4 | **REJECT** | Distributed → Phase 02, 09, 10, 12; threat modeling → extension rec | — |
| Logging & Observability | R2 | **REJECT** | Distributed → Phase 12, 09, 13; trace IDs → extension rec | — |
| Dependency & Build Reproducibility | R4 | **REJECT** | Covered by Phase 12 blocks 4, 1, 2, 7 | — |
