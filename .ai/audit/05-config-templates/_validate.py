"""Scratch audit reproduction for Phase 05 — NOT production code.

JSON5-MIGRATION TODO: This audit script uses `yaml.safe_load`, `glob("**/*.yaml")`,
and imports `yaml_to_dict`/`en_to_ru` from `core.utils` — all stale after the JSON5
settings templates migration (yaml_to_dict → json5_to_dict, .yaml → .json5,
yaml.safe_load → json5.load, PyYAML → json5). Reconcile in a future audit pass;
does NOT block the implementation PR.

Validates bundled JSON5 settings templates against the Pydantic models in
core/models.py and reproduces the defects filed in findings.md.

Repro: uv run python .ai/audit/05-config-templates/_validate.py
"""
import sys
from pathlib import Path
import yaml

ROOT = Path.cwd()
SETTINGS = ROOT / "src/mko_get_mediascope_data/settings"
sys.path.insert(0, str(ROOT / "src"))

from mko_get_mediascope_data.core.models import (
    Data, DataDefaults, LastTimeModel, PeriodModel, ReportSettings,
)
from mko_get_mediascope_data.core.utils import yaml_to_dict, en_to_ru
from mko_get_mediascope_data.core.services import AppService
from mko_get_mediascope_data.core.paths import APP_PATHS, PathResolver

svc = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.app_dir))


def load_yaml(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


print("=" * 70)
print("BLOCK 1: Report templates -> ReportSettings")
print("=" * 70)
total = fails = 0
sig_count = {}
for rf in sorted(SETTINGS.glob("reports/**/*.yaml")):
    data = load_yaml(rf) or []
    if isinstance(data, dict):
        data = [data]
    for i, entry in enumerate(data):
        total += 1
        try:
            ReportSettings(**entry)
        except Exception as e:
            fails += 1
            sig = "; ".join(
                f"{'.'.join(str(x) for x in err['loc'])}: {err['type']}"
                for err in e.errors()
            )
            sig_count.setdefault(sig, []).append(f"{rf.relative_to(SETTINGS)}[{i}]")
            print(f"  FAIL {rf.relative_to(SETTINGS)}[{i}]: {sig}")
print(f"\n  total entries validated: {total}\n  failed: {fails}\n  passed: {total - fails}")

print("\n" + "=" * 70)
print("BLOCK 2: Default data templates -> DataDefaults")
print("=" * 70)
for df in sorted(SETTINGS.glob("defaults/**/*.yaml")):
    try:
        DataDefaults(**load_yaml(df))
        print(f"  OK   {df.relative_to(SETTINGS)}")
    except Exception as e:
        print(f"  FAIL {df.relative_to(SETTINGS)}: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("BLOCK 5: per-model extra policy")
print("=" * 70)
for m in (LastTimeModel, PeriodModel, ReportSettings, DataDefaults, Data):
    print(f"  {m.__name__}: extra={m.model_config.get('extra')!r}")

print("\n" + "=" * 70)
print("BLOCK 2+4: merge -> Data for the only validating template (nat_tv_brands_tefal[0])")
print("=" * 70)
tefal = yaml_to_dict(SETTINGS / "reports/nat_tv_brands_tefal.yaml")
rs = ReportSettings(**tefal[0])
dd = svc.get_default_data_settings(rs)
merged = svc.merge_data_settings(dd, rs)
data = Data(**merged)
print(f"  merged keys count: {len(merged)}  declared Data fields: 3 (options, statistics, slices)")
print(f"  Data.model_dump() keys count: {len(data.model_dump())}")
print(f"  ad_filter survived from template: {merged.get('ad_filter')[:40]}...")
print(f"  options.kitId survived from default DYNAMICS_BY_SPOTS: {merged['options'].get('kitId')}")

print("\n" + "=" * 70)
print("BLOCK 3+4: en_to_ru(None) crash path")
print("=" * 70)
try:
    en_to_ru(None)
    print("  en_to_ru(None) -> OK (unexpected)")
except Exception as e:
    print(f"  en_to_ru(None) raises: {type(e).__name__}: {e}")
# Trigger via the real merge path for a ru-language report whose subtype
# has no defaults block.
probe = ReportSettings(**{
    "media": "TV", "report_type": "crosstab",
    "report_subtype": "NAT_TV_CHANNELS_ATV", "data_lang": "ru",
    "relative_path": "x", "ad_filter": None,
    "target_audiences": {"all 18+": "age >= 18"},
    "period": {"date_filter": ["2025-01-01", "2025-01-07"], "last_time": None},
})
try:
    svc.get_data_settings(probe)
    print("  get_data_settings(NAT_TV_CHANNELS_ATV/ru) -> OK (unexpected)")
except Exception as e:
    print(f"  get_data_settings(NAT_TV_CHANNELS_ATV/ru) crashes: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("BLOCK 4 (latent): missing tv_reg/timeband.yaml default")
print("=" * 70)
missing = APP_PATHS.app_settings_dir / "defaults/tv_reg/timeband.yaml"
print(f"  {missing.relative_to(SETTINGS)} exists={missing.exists()}")
shipped = sorted(p.relative_to(SETTINGS / "defaults") for p in SETTINGS.glob("defaults/**/*.yaml"))
print(f"  shipped defaults: {[str(p) for p in shipped]}")

print("\n  subtype blocks present in tv/crosstab.yaml:")
dd_tv = svc.get_default_data_settings(
    ReportSettings(**{"media": "TV", "report_type": "crosstab",
                       "report_subtype": "TOP_NAT_TV_ADVERTISERS",
                       "data_lang": "en", "relative_path": "x",
                       "period": {"date_filter": ["2025-01-01", "2025-01-07"], "last_time": None}})
)
print(f"  {sorted(k for k in dd_tv.__pydantic_extra__ if k != 'DATA_DEFAULTS')}")
