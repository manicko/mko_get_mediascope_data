import asyncio
import sys
from pathlib import Path

from mko_get_mediascope_data.core.paths import APP_DIR as PACKAGE_APP_DIR
from mko_get_mediascope_data.core.paths import APP_NAME, AppPaths, PathResolver
from mko_get_mediascope_data.core.services import AppService

REPORT_FILENAME = "nat_tv_brands_theiss_weekly.json5"


async def main() -> int:
    workspace_root = Path.cwd()
    report_path = workspace_root / "settings" / "reports" / REPORT_FILENAME
    export_dir = workspace_root.parent

    app_paths = AppPaths(user_dir=workspace_root, app_dir=PACKAGE_APP_DIR, app_name=APP_NAME)
    service = AppService(app_paths, PathResolver(workspace_root))

    try:
        export_paths = await service.run_report(report_path, export_folder=export_dir)
    except Exception as exc:
        print(file=sys.stderr)
        print("STATUS: FAILED - process finished with errors.", file=sys.stderr)
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print()
    print("STATUS: SUCCESS - process finished without errors.")
    print(f"Export targets: {len(export_paths)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
