"""Phase 11 mutation probe — audit scratch script.

Runs a curated set of mutations against production source to test whether
the test suite actually catches regressions. Restores each file to HEAD
after each mutation so the worktree stays clean.

Run from repo root:
    uv run python .ai/audit/11-test-coverage/_probe_mutations.py
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "src" / "mko_get_mediascope_data" / "core"
TESTS = ROOT / "tests"
TIMEOUT = 240  # seconds — never hang like M11 did

# (id, file, old_snippet, new_snippet, description)
MUTATIONS = [
    # M10 — re-confirm the done-files skip branch (reports.py:378-380)
    (
        "M10", "reports.py",
        '                if task.name in report.done_files:\n                    logger.warning(f"File \'{task.name}\' already exists in folder, skipping.")\n                    continue\n',
        '                if task.name in report.done_files:\n                    logger.warning(f"File \'{task.name}\' already exists in folder, skipping.")\n                    yield None  # MUTATION: continue -> yield None\n                    continue\n',
        "DefaultTVTaskStrategy done-file skip: continue -> yield None"
    ),
    # M13 — network.py: max_attempts default 10 -> 1 (retry loop effectively disabled)
    (
        "M13", "network.py",
        '        max_attempts: int = 10,',
        '        max_attempts: int = 1,  # MUTATION: retry disabled',
        "NetworkClient: max_attempts 10 -> 1"
    ),
    # M16 — models.py: PeriodModel exclusive validator loosened
    (
        "M16", "models.py",
        '    @model_validator(mode="after")\n    def validate_exclusive(self):\n        if not self.date_filter and not self.last_time:\n            raise ValueError("Specify either date_filter or last_time")\n\n        if self.date_filter and len(self.date_filter) != 2:\n            raise ValueError("date_filter must contain 2 dates")\n\n        return self',
        '    @model_validator(mode="after")\n    def validate_exclusive(self):\n        return self  # MUTATION: validator body deleted',
        "PeriodModel.validate_exclusive neutralized"
    ),
    # M17 — models.py: multiple_files default flipped to False
    (
        "M17", "models.py",
        '    multiple_files: bool = (\n        True  # True: if period exceeds 1 year, split exports by month/week\n    )',
        '    multiple_files: bool = (\n        False  # MUTATION: default flipped\n    )',
        "ReportSettings.multiple_files default: True -> False"
    ),
]


def apply(src_file: Path, old: str, new: str) -> None:
    text = src_file.read_text(encoding="utf-8")
    if old not in text:
        raise AssertionError(f"anchor not found in {src_file.name}")
    src_file.write_text(text.replace(old, new, 1), encoding="utf-8")


def run_tests() -> tuple[str, str, int]:
    res = subprocess.run(
        [sys.executable, "-m", "pytest", str(TESTS), "-x", "-q"],
        capture_output=True, text=True,
        cwd=str(ROOT), timeout=TIMEOUT,
    )
    return res.stdout[-2000:], res.stderr[-1000:], res.returncode


def restore(file: Path) -> str:
    subprocess.run(["git", "checkout", "--", str(file)], cwd=str(ROOT))
    diff = subprocess.run(
        ["git", "diff", "--stat", "HEAD", "--", str(file)],
        capture_output=True, text=True, cwd=str(ROOT),
    ).stdout.strip()
    return diff or "(clean vs HEAD)"


def main() -> int:
    results: list[tuple[str, str, str, str]] = []
    for mut_id, fname, old, new, desc in MUTATIONS:
        src_file = SRC / fname
        backup = src_file.read_text(encoding="utf-8")
        try:
            apply(src_file, old, new)
        except AssertionError as e:
            restore(src_file)
            results.append((mut_id, "ANCHOR_MISSING", desc, str(e)))
            continue
        try:
            out, err, rc = run_tests()
        except subprocess.TimeoutExpired:
            results.append((mut_id, "TIMEOUT", desc, f"pytest hung > {TIMEOUT}s"))
        else:
            verdict = "DETECTED" if rc != 0 else "NOT DETECTED"
            tail = (out + err)[-300:]
            results.append((mut_id, verdict, desc, tail))
        finally:
            restore(src_file)
    # report
    print("\n" + "=" * 70)
    print("MUTATION PROBE RESULTS")
    print("=" * 70)
    for mut_id, verdict, desc, detail in results:
        print(f"\n[{mut_id}] {verdict} — {desc}")
        # compact detail: show only failure-indicating lines
        lines = [l for l in detail.splitlines() if "FAILED" in l or "error" in l.lower() or "assert" in l.lower()]
        for l in (lines or detail.splitlines()[-3:]):
            print(f"  | {l.rstrip()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
