"""Runtime evidence for phase 04 (data-pipeline) audit repro steps."""
import asyncio
import logging
import shutil
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

# Capture core logger output to inspect messages
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

from mko_get_mediascope_data.core.models import Data, ReportSettings
from mko_get_mediascope_data.core.reports import (
    DefaultDataStrategy,
    DefaultTVTaskStrategy,
    MediaReport,
    TVMediaReport,
    TVRegTaskStrategy,
)
from mko_get_mediascope_data.core.tasks import TVTask
from mko_get_mediascope_data.core.utils import csv_to_file, slice_period, get_last_period

print("=== REPRO A: get_period crashes with last_time=None + explicit date_filter ===")
settings = ReportSettings.model_validate({
    "media": "TV",
    "report_type": "crosstab",
    "relative_path": "tv/test",
    "multiple_files": True,
    "period": {"date_filter": ["2025-01-01", "2025-01-07"], "last_time": None},
    "target_audiences": {"all 18+": "age >= 18"},
})
data_settings = Data(options={"kitId": 1}, statistics=["RtgPerSum"], slices=["researchDate"])
report_service = MagicMock()
network_client = AsyncMock()
network_client.call = AsyncMock(return_value=pd.DataFrame(
    {"id": ["1"], "periodFrom": ["2024-01-01"], "periodTo": ["2025-12-31"]}
))
rep = TVMediaReport(
    report_settings=settings, data_settings=data_settings,
    export_path=Path(tempfile.mkdtemp()), report_service=report_service,
    network_client=network_client, check_done=False,
)
try:
    period = asyncio.run(rep.get_period())
    print("  RESULT:", period)
except Exception as e:
    print("  CRASH:", type(e).__name__, "->", e)

print()
print("=== REPRO B: to_yaml target directory (absolute export_path) ===")
tmp = Path(tempfile.mkdtemp())
abs_export = tmp / "output" / "reports" / "finance" / "raw_data"
abs_export.mkdir(parents=True)
task = TVTask(name="period_target", settings={"date_filter": [["2025-01-01", "2025-01-07"]], "basedemo_filter": "age >= 18"},
              report_subtype="default", report_type="crosstab")
task.target = "all 18+"; task.interval = ("2025-01-01", "2025-01-07"); task.error = "boom"
task.to_yaml(abs_export)
cwd = Path().absolute()
candidates = {
    "export/errors/name.yaml": abs_export / "errors" / "period_target.yaml",
    "cwd/data/output/<export>/errors/name.yaml": cwd / "data" / "output" / str(abs_export) / "errors" / "period_target.yaml",
    "cwd/errors/name.yaml": cwd / "errors" / "period_target.yaml",
}
for label, p in candidates.items():
    print(f"  exists [{label}]? {p.exists()}  (path={p})")
# cleanup any stray file actually created
for p in candidates.values():
    if p.exists():
        print("  ACTUAL file written to:", p.resolve())

print()
print("=== REPRO B2: to_yaml with a RELATIVE folder (latent path) ===")
rel = "rel_out"
task.to_yaml(rel)
for label, p in {
    "cwd/data/output/rel_out/errors/name.yaml": (cwd / "data" / "output" / rel / "errors" / "period_target.yaml"),
    "cwd/rel_out/errors/name.yaml": (cwd / rel / "errors" / "period_target.yaml"),
}.items():
    print(f"  exists [{label}]? {p.exists()}")
shutil.rmtree(cwd / "data", ignore_errors=True)
shutil.rmtree(cwd / rel, ignore_errors=True)

print()
print("=== REPRO C: csv_to_file mode='x' swallowed -> caller sees success ===")
out = tmp / "csvout"; out.mkdir()
df = pd.DataFrame({"col": [1, 2]})
csv_to_file(df, out, file_prefix="report1", add_time=False, compression={"method": "gzip"})
files1 = list(out.glob("*.csv.gz"))
print("  1st write -> files:", [f.name for f in files1])
# second call with same prefix -> FileExistsError is swallowed inside csv_to_file
raised = None
try:
    csv_to_file(df, out, file_prefix="report1", add_time=False, compression={"method": "gzip"})
except Exception as e:
    raised = e
files2 = list(out.glob("*.csv.gz"))
print("  2nd write raised:", raised)
print("  files after 2nd:", len(files2), "(swallowed FileExistsError -> no exception to caller)")

print()
print("=== REPRO D: worker hangs when send_task raises (task_done never called) ===")
mr = MediaReport(
    report_settings=settings, data_settings=data_settings,
    export_path=tmp / "w", report_service=MagicMock(),
    network_client=MagicMock(), max_tasks=1, check_done=False,
)
mr.task_queue = asyncio.Queue(maxsize=1)


async def raising_send(task):
    raise RuntimeError("boom from API send (e.g. BadRequestError after retries)")


mr.send_task = raising_send
mr.wait_task = AsyncMock()
mr.extract_data = AsyncMock()

async def run():
    worker = asyncio.create_task(mr.worker())
    await mr.task_queue.put(TVTask("t", {}, "d", "crosstab"))
    try:
        await asyncio.wait_for(mr.task_queue.join(), timeout=2)
        print("  join returned (unexpected)")
    except asyncio.TimeoutError:
        print("  queue.join() HUNG >2s (task_done never called -> worker raised before line 153)")
    worker.cancel()
    try:
        await worker
    except (asyncio.CancelledError, RuntimeError):
        pass

asyncio.run(run())

print()
print("=== REPRO E: wait_task has no upper bound (while True, no deadline) ===")
import inspect
from mko_get_mediascope_data.core.reports import TVMediaReport as R2
src = inspect.getsource(R2.wait_task)
print("  wait_task source contains 'while True':", "while True" in src)
print("  wait_task source contains 'max_attempts' or 'deadline' or 'timeout':",
      any(k in src for k in ("max_attempts", "deadline", "timeout", "max_iter", "time.monotonic")))

print()
print("=== REPRO F: zero-task report (empty target_audiences) ===")
settings_empty = ReportSettings.model_validate({
    "media": "TV", "report_type": "crosstab", "relative_path": "tv/test",
    "multiple_files": True,
    "period": {"date_filter": ["2025-01-01", "2025-01-07"], "last_time": {"unit_count": 1}, "time_unit": "m"},
    "target_audiences": {},
})
rep2 = TVMediaReport(report_settings=settings_empty, data_settings=data_settings,
                     export_path=tmp / "z", report_service=report_service,
                     network_client=network_client, check_done=False)
# bypass get_available_period by setting period manually
rep2.period = [("2025-01-01", "2025-01-07")]
strat = DefaultTVTaskStrategy()
async def _consume():
    return [t async for t in strat.generate(rep2)]
tasks = asyncio.run(_consume())
print("  zero-task report -> generated tasks count:", len(tasks), "(no output, no error)")

print()
print("=== REPRO G: KeyError logged without task name/interval ===")
strat2 = DefaultDataStrategy()
report_mock = MagicMock()
report_mock.output_columns = {"slices": ["missing_col"], "statistics": ["stat_x"]}
df_ok = pd.DataFrame({"other": [1]})
records = []
class H(logging.Handler):
    def emit(self, r): records.append(r.getMessage())
reports_logger = logging.getLogger("mko_get_mediascope_data.core.reports")
h = H(); reports_logger.addHandler(h)
res = asyncio.run(strat2.prepare(report_mock, df_ok))
print("  prepare returned:", res)
print("  log records:", records)
print("  (column name present in err, but NO task name / interval in message)")

print()
print("=== REPRO H: get_period silent clamp (no warning logged) ===")
settings_far = ReportSettings.model_validate({
    "media": "TV", "report_type": "crosstab", "relative_path": "tv/test",
    "multiple_files": False,
    "period": {"date_filter": ["1900-01-01", "1900-02-01"], "last_time": {"unit_count": 1}, "time_unit": "m"},
    "target_audiences": {"all 18+": "age >= 18"},
})
rep3 = TVMediaReport(report_settings=settings_far, data_settings=data_settings,
                     export_path=tmp / "c", report_service=report_service,
                     network_client=network_client, check_done=False)
avail = asyncio.run(rep3.get_available_period())
print("  available_period from API:", avail)
period = asyncio.run(rep3.get_period())
print("  resolved period (clamped):", period, "(requested 1900-01-01..1900-02-01 -> truncated to API range, no warning)")

shutil.rmtree(tmp, ignore_errors=True)
print("\nDONE")
