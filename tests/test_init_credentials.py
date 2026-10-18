import json

import pytest
from pathlib import Path
from unittest.mock import patch

from mko_get_mediascope_data.core.init_service import (
    _copy_file_atomic,
    _migrate_old_credential_name,
    init_project,
)
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths


EXPECTED_TEMPLATE_FILES = [
    "app_config.json5",
    "connections/mediascope.json",
    "defaults/tv/crosstab.json5",
    "defaults/tv/simple.json5",
    "defaults/tv/timeband.json5",
    "defaults/tv_reg/crosstab.json5",
    "defaults/tv_reg/simple.json5",
    "defaults/tv_reg/timeband.json5",
    "log_config.json5",
    "reports/gap_resurs/nat_tv_brands.json5",
    "reports/gap_resurs/reg_tv_by_spots.json5",
    "reports/last_category/nat_tv_brands_last.json5",
    "reports/last_category/nat_tv_brands_snack.json5",
    "reports/last_category/nat_tv_brands_weekly_reach.json5",
    "reports/nat_tv_brands_last.json5",
    "reports/nat_tv_brands_tefal.json5",
    "reports/nat_tv_tvreport.json5",
    "reports/sovcombank/nat_tv_brands_finance.json5",
    "reports/sovcombank/nat_tv_brands_finance2.json5",
    "reports/sovcombank/nat_tv_by_spots.json5",
    "reports/sovcombank/nat_tv_finance_q.json5",
    "reports/sovcombank/reg_tv_by_spots.json5",
    "reports/sovcombank/tv_msk_new_creatives.json5",
    "reports/sovcombank/tv_msk_new_creatives_short.json5",
    "reports/test.json5",
]

EXPECTED_CRED_KEYS = {
    "username", "passw", "client_id", "client_secret",
    "auth_server", "root_url",
}


def test_init_produces_readable_credential_file(tmp_path: Path) -> None:
    """After init_project(), connections/mediascope.json must exist and parse as JSON."""
    app_paths = AppPaths(
        app_dir=APP_PATHS.app_dir, app_name="test", user_dir=tmp_path
    )

    import mko_get_mediascope_data.core.init_service as init_mod

    with patch.object(init_mod, "APP_PATHS", app_paths):
        init_project()

    cred_file = tmp_path / "settings" / "connections" / "mediascope.json"
    assert cred_file.exists(), f"Credential file not found at {cred_file}"

    data = json.loads(cred_file.read_text(encoding="utf-8"))
    assert EXPECTED_CRED_KEYS.issubset(data.keys()), (
        f"Credential file missing keys: {EXPECTED_CRED_KEYS - set(data.keys())}"
    )


def test_init_returns_settings_path(tmp_path: Path) -> None:
    """init_project() returns the user_settings_dir path."""
    app_paths = AppPaths(
        app_dir=APP_PATHS.app_dir, app_name="test", user_dir=tmp_path
    )

    import mko_get_mediascope_data.core.init_service as init_mod

    with patch.object(init_mod, "APP_PATHS", app_paths):
        result = init_project()

    assert result == tmp_path / "settings"


def test_init_copies_all_template_files(tmp_path: Path) -> None:
    """All 25 bundled template files must exist after init_project()."""
    app_paths = AppPaths(
        app_dir=APP_PATHS.app_dir, app_name="test", user_dir=tmp_path
    )

    import mko_get_mediascope_data.core.init_service as init_mod

    with patch.object(init_mod, "APP_PATHS", app_paths):
        init_project()

    for rel in EXPECTED_TEMPLATE_FILES:
        assert (tmp_path / "settings" / rel).exists(), (
            f"Template file not copied: {rel}"
        )


def test_init_force_overwrites_existing_file(tmp_path: Path) -> None:
    """init_project(force=True) overwrites pre-existing files (skip branch bypassed)."""
    app_paths = AppPaths(
        app_dir=APP_PATHS.app_dir, app_name="test", user_dir=tmp_path
    )

    # Pre-create the settings dir + a stale file
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    stale_file = settings_dir / "log_config.json5"
    stale_file.write_text("STALE CONTENT")

    import mko_get_mediascope_data.core.init_service as init_mod

    with patch.object(init_mod, "APP_PATHS", app_paths):
        init_project(force=True)

    # The stale file must have been overwritten by the template copy
    content = stale_file.read_text(encoding="utf-8")
    assert content != "STALE CONTENT"
    assert "version" in content  # template content has logging "version" key


def test_copy_file_atomic_cleans_up_temp_on_failure(tmp_path: Path) -> None:
    """_copy_file_atomic removes the temp file if shutil.copy2 fails."""
    source = tmp_path / "source.json5"
    source.write_text("data")

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target_file = target_dir / "copied.json5"

    def fail_copy2(src, dst, *args, **kwargs):
        raise OSError("disk full")

    with patch("shutil.copy2", side_effect=fail_copy2):
        with pytest.raises(OSError, match="disk full"):
            _copy_file_atomic(source, target_file)

    # The target must not exist (copy failed before os.replace)
    assert not target_file.exists()

    # Temp files created by tempfile.mkstemp may linger in some failure
    # scenarios, but the function must clean up on shutil.copy2 failure.
    temp_files = list(target_dir.glob("*"))
    assert len(temp_files) == 0


def test_migrate_old_credential_name_renames_legacy(tmp_path: Path) -> None:
    """Legacy mediascope_RENAME_.json is renamed to mediascope.json."""
    connections = tmp_path / "connections"
    connections.mkdir()
    old_path = connections / "mediascope_RENAME_.json"
    old_path.write_text("legacy credentials")

    _migrate_old_credential_name(tmp_path)

    new_path = connections / "mediascope.json"
    assert new_path.exists()
    assert not old_path.exists()
    assert new_path.read_text() == "legacy credentials"


def test_migrate_old_credential_name_warns_when_both_exist(tmp_path: Path) -> None:
    """When both legacy and primary exist, legacy is left untouched (warning logged)."""
    connections = tmp_path / "connections"
    connections.mkdir()
    (connections / "mediascope_RENAME_.json").write_text("legacy")
    (connections / "mediascope.json").write_text("primary")

    _migrate_old_credential_name(tmp_path)

    # Legacy file remains untouched
    assert (connections / "mediascope_RENAME_.json").exists()
    # Primary file unchanged
    assert (connections / "mediascope.json").read_text() == "primary"


def test_migrate_old_credential_name_noop_when_no_legacy(tmp_path: Path) -> None:
    """When the legacy file does not exist, the function returns without error."""
    connections = tmp_path / "connections"
    connections.mkdir()

    # Should be a no-op
    _migrate_old_credential_name(tmp_path)

    assert not (connections / "mediascope.json").exists()
    assert not (connections / "mediascope_RENAME_.json").exists()
