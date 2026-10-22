"""Subprocess tests for ``ci/check_release_version.py`` using stdlib-only fixtures.

Each test constructs its own minimal wheel (zip) and/or sdist (tar.gz) artifacts
under ``tmp_path`` and invokes the real guard script via ``subprocess``. The
repository's real ``dist/`` directory is never read, and neither ``setuptools-scm``
nor an actual package build is required.
"""

from __future__ import annotations

import io
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "ci" / "check_release_version.py"

_PKG = "mko_get_mediascope_data"


def _metadata_text(version: str, *, pkg: str = _PKG, include_version: bool = True) -> str:
    """Return minimal RFC-822 metadata with an optional ``Version:`` header."""
    lines = [
        "Metadata-Version: 2.1",
        f"Name: {pkg}",
    ]
    if include_version:
        lines.append(f"Version: {version}")
    return "\n".join(lines) + "\n"


def _write_wheel(
    dist_dir: Path,
    version: str,
    *,
    pkg: str = _PKG,
    include_version: bool = True,
    extra_metadata_members: int = 0,
    filename_version: str | None = None,
) -> Path:
    """Write a minimal wheel with one ``<pkg>-<ver>.dist-info/METADATA`` member."""
    file_version = filename_version if filename_version is not None else version
    wheel = dist_dir / f"{pkg}-{file_version}-py3-none-any.whl"
    metadata_member = f"{pkg}-{file_version}.dist-info/METADATA"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            metadata_member,
            _metadata_text(version, pkg=pkg, include_version=include_version),
        )
        for extra in range(extra_metadata_members):
            archive.writestr(
                f"extra{extra}-{file_version}.dist-info/METADATA",
                _metadata_text(version, pkg=pkg, include_version=include_version),
            )
    return wheel


def _write_sdist(
    dist_dir: Path,
    version: str,
    *,
    pkg: str = _PKG,
    include_version: bool = True,
    with_nested_egg_info: bool = True,
    with_top_level: bool = True,
    extra_top_level_pkg_info: int = 0,
    filename_version: str | None = None,
) -> Path:
    """Write a minimal sdist mirroring the real two-``PKG-INFO`` layout.

    By default it contains a single top-level ``<pkg>-<ver>/PKG-INFO`` plus a
    nested ``<pkg>-<ver>/src/<pkg>.egg-info/PKG-INFO`` member (which the guard
    must ignore).
    """
    file_version = filename_version if filename_version is not None else version
    sdist = dist_dir / f"{pkg}-{file_version}.tar.gz"
    root = f"{pkg}-{file_version}"
    content = _metadata_text(version, pkg=pkg, include_version=include_version)

    def _add(archive: tarfile.TarFile, name: str, text: str) -> None:
        data = text.encode("utf-8")
        info = tarfile.TarInfo(name)
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))

    with tarfile.open(sdist, "w:gz") as archive:
        if with_top_level:
            _add(archive, f"{root}/PKG-INFO", content)
        for extra in range(extra_top_level_pkg_info):
            _add(archive, f"{root}-extra{extra}/PKG-INFO", content)
        if with_nested_egg_info:
            _add(archive, f"{root}/src/{pkg}.egg-info/PKG-INFO", content)
        _add(archive, f"{root}/pyproject.toml", "[build-system]\n")
    return sdist


def _run(dist_dir: Path, tag: str) -> subprocess.CompletedProcess[str]:
    """Invoke the real guard script against ``dist_dir`` with ``tag``."""
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--tag", tag, "--dist-dir", str(dist_dir)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_release_version_check_accepts_dynamic_metadata_and_rejects_mismatch(
    tmp_path: Path,
) -> None:
    """Matching artifacts+tag -> 0; differing tag -> non-zero; bare tag accepted."""
    _write_wheel(tmp_path, "1.2.3")
    _write_sdist(tmp_path, "1.2.3")

    matching = _run(tmp_path, "v1.2.3")
    assert matching.returncode == 0, matching.stderr
    assert "1.2.3" in matching.stdout

    bare_tag = _run(tmp_path, "1.2.3")
    assert bare_tag.returncode == 0, bare_tag.stderr

    mismatched = _run(tmp_path, "v9.9.9")
    assert mismatched.returncode != 0
    assert mismatched.stderr.strip()
    assert "9.9.9" in mismatched.stderr


def test_release_version_check_fails_on_inconsistent_versions(tmp_path: Path) -> None:
    """Wheel and sdist versions disagree -> non-zero even with a matching tag."""
    _write_wheel(tmp_path, "1.2.3")
    _write_sdist(tmp_path, "1.2.4")

    result = _run(tmp_path, "v1.2.3")
    assert result.returncode != 0
    assert result.stderr.strip()
    assert "1.2.3" in result.stderr and "1.2.4" in result.stderr


def test_release_version_check_fails_on_missing_wheel_metadata(tmp_path: Path) -> None:
    """No wheel, or an unexpected number of ``*.dist-info/METADATA`` -> non-zero."""
    _write_sdist(tmp_path, "1.2.3")
    no_wheel = _run(tmp_path, "v1.2.3")
    assert no_wheel.returncode != 0
    assert "wheel" in no_wheel.stderr.lower()

    empty_metadata_dir = tmp_path / "empty-metadata"
    empty_metadata_dir.mkdir()
    _write_wheel(empty_metadata_dir, "1.2.3", extra_metadata_members=0)
    # Remove the single METADATA member so the wheel has zero metadata members.
    wheel = next(empty_metadata_dir.glob("*.whl"))
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(f"{_PKG}-1.2.3.dist-info/WHEEL", "Wheel-Version: 1.0\n")
    _write_sdist(empty_metadata_dir, "1.2.3")
    zero = _run(empty_metadata_dir, "v1.2.3")
    assert zero.returncode != 0
    assert zero.stderr.strip()

    two_metadata_dir = tmp_path / "two-metadata"
    two_metadata_dir.mkdir()
    _write_wheel(two_metadata_dir, "1.2.3", extra_metadata_members=1)
    _write_sdist(two_metadata_dir, "1.2.3")
    two = _run(two_metadata_dir, "v1.2.3")
    assert two.returncode != 0
    assert two.stderr.strip()


def test_release_version_check_fails_on_missing_top_level_pkg_info(
    tmp_path: Path,
) -> None:
    """Only a nested egg-info PKG-INFO -> non-zero; two top-level members -> non-zero."""
    nested_only = tmp_path / "nested-only"
    nested_only.mkdir()
    _write_wheel(nested_only, "1.2.3")
    _write_sdist(nested_only, "1.2.3", with_top_level=False, with_nested_egg_info=True)
    nested_result = _run(nested_only, "v1.2.3")
    assert nested_result.returncode != 0
    assert "PKG-INFO" in nested_result.stderr

    two_top_level = tmp_path / "two-top-level"
    two_top_level.mkdir()
    _write_wheel(two_top_level, "1.2.3")
    _write_sdist(two_top_level, "1.2.3", extra_top_level_pkg_info=1)
    two_result = _run(two_top_level, "v1.2.3")
    assert two_result.returncode != 0
    assert "PKG-INFO" in two_result.stderr


def test_release_version_check_fails_on_no_artifacts(tmp_path: Path) -> None:
    """An empty dist directory -> non-zero naming the missing artifacts."""
    result = _run(tmp_path, "v1.2.3")
    assert result.returncode != 0
    assert result.stderr.strip()
    assert "wheel" in result.stderr.lower()


def test_release_version_check_fails_on_ambiguous_artifacts(tmp_path: Path) -> None:
    """Two wheels/sdists -> non-zero; a missing Version header -> non-zero."""
    two_wheels = tmp_path / "two-wheels"
    two_wheels.mkdir()
    _write_wheel(two_wheels, "1.2.3")
    _write_wheel(two_wheels, "1.2.3", filename_version="1.2.3-alt")
    _write_sdist(two_wheels, "1.2.3")
    ambiguous_wheels = _run(two_wheels, "v1.2.3")
    assert ambiguous_wheels.returncode != 0
    assert "wheel" in ambiguous_wheels.stderr.lower()

    two_sdists = tmp_path / "two-sdists"
    two_sdists.mkdir()
    _write_wheel(two_sdists, "1.2.3")
    _write_sdist(two_sdists, "1.2.3")
    _write_sdist(two_sdists, "1.2.3", filename_version="1.2.3-alt")
    ambiguous_sdists = _run(two_sdists, "v1.2.3")
    assert ambiguous_sdists.returncode != 0
    assert "sdist" in ambiguous_sdists.stderr.lower()

    missing_header = tmp_path / "missing-header"
    missing_header.mkdir()
    _write_wheel(missing_header, "1.2.3", include_version=False)
    _write_sdist(missing_header, "1.2.3")
    header_result = _run(missing_header, "v1.2.3")
    assert header_result.returncode != 0
    assert "Version" in header_result.stderr
