from pathlib import Path
from unittest.mock import patch

from mko_get_mediascope_data.core.init_service import init_project
from mko_get_mediascope_data.core.paths import APP_PATHS, AppPaths


def test_init_produces_readable_credential_file(tmp_path: Path) -> None:
    """After init_project(), connections/mediascope.json must exist."""
    app_paths = AppPaths(
        app_dir=APP_PATHS.app_dir, app_name="test", user_dir=tmp_path
    )

    # Temporarily set APP_PATHS so init_project can copy templates
    import mko_get_mediascope_data.core.init_service as init_mod

    with patch.object(init_mod, "APP_PATHS", app_paths):
        init_project()

    cred_file = tmp_path / "settings" / "connections" / "mediascope.json"
    assert cred_file.exists(), f"Credential file not found at {cred_file}"
    assert app_paths.connection_config.exists()
