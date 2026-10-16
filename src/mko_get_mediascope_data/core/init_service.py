import logging
import os
import shutil
import tempfile
from pathlib import Path

from mko_get_mediascope_data.core.paths import APP_PATHS

logger = logging.getLogger(__name__)


def _copy_file_atomic(source: Path, target_path: Path) -> None:
    """Copy ``source`` to ``target_path`` atomically.

    Writes to a temporary file located in the same parent directory as
    ``target_path`` (same filesystem is required for ``os.replace`` to be
    atomic), then renames the temp file into place with ``os.replace``.

    A copy interrupted at any point never leaves a partial file at
    ``target_path``: either the previous content remains or the complete new
    content appears in a single atomic step.
    """
    fd, tmp_path = tempfile.mkstemp(dir=str(target_path.parent))
    os.close(fd)

    try:
        shutil.copy2(source, tmp_path)
        os.replace(tmp_path, target_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            logger.debug("Could not remove temp file %s", tmp_path, exc_info=True)
        raise


def _copy_templates(destination: Path, force: bool = False) -> None:
    """
    Copy template files from package settings directory to user settings directory.
    """

    source_dir = Path(APP_PATHS.app_settings_dir)
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
        _copy_file_atomic(item, target_path)


def _migrate_old_credential_name(user_settings_dir: Path) -> None:
    """Rename legacy mediascope_RENAME_.json to mediascope.json.

    Runs before _copy_templates so the migration preserves real
    user credentials that were in the old file. If mediascope.json
    already exists (user renamed or re-init with force), the old
    file is left untouched and a warning is logged.
    """
    connections_dir = user_settings_dir / "connections"
    old_path = connections_dir / "mediascope_RENAME_.json"
    new_path = connections_dir / "mediascope.json"

    if not old_path.exists():
        return

    if new_path.exists():
        logger.warning(
            "Legacy credential file '%s' still exists alongside '%s'. "
            "Using '%s'. Please remove the legacy file manually.",
            old_path,
            new_path,
            new_path,
        )
        return

    old_path.rename(new_path)
    logger.info(
        "Renamed legacy credential file '%s' to '%s'.",
        old_path,
        new_path,
    )


def init_project(force: bool = False) -> Path:
    """
    Initialize configuration directory with default templates.

    Args:
        force: Overwrite existing files.

    Returns:
        Path to created config directory.
    """

    _migrate_old_credential_name(APP_PATHS.user_settings_dir)
    _copy_templates(APP_PATHS.user_settings_dir, force)
    return APP_PATHS.user_settings_dir


if __name__ == "__main__":
    init_project(force=True)
