from pathlib import Path
import shutil
from mko_get_mediascope_data.core.path_service import PATHS



def _copy_templates(destination: Path, force: bool = False) -> None:
    """
    Copy template files from package settings directory to user settings directory.
    """

    source_dir = Path(PATHS.module_settings_dir)
    destination.mkdir(parents=True, exist_ok=True)

    for item in source_dir.rglob("*"):
        relative_path = item.relative_to(source_dir)
        target_path = destination / relative_path

        if item.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue

        if target_path.exists() and not force:
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target_path)


def init_project(force: bool = False)-> Path:
    """
    Initialize configuration directory with default templates.

    Args:
        force: Overwrite existing files.

    Returns:
        Path to created config directory.
    """

    _copy_templates(PATHS.user_settings_dir, force)
    return PATHS.user_settings_dir



if __name__ == "__main__":
    init_project()
