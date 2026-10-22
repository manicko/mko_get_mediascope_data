"""Release version guard: verify built-artifact version matches the git tag.

Reads the distribution version from the ACTUAL built artifacts (wheel
``*.dist-info/METADATA`` and sdist top-level ``PKG-INFO``), requires the wheel
and sdist to agree, and compares the agreed version to the release tag after
stripping a single leading ``v``. Fails closed on any missing, ambiguous, or
absent metadata with a diagnostic on stderr and a non-zero exit code.

This is release glue, not shipped library code: it is stdlib-only so it can run
under a bare CI Python, and it must never live under ``src/``.
"""

from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from collections.abc import Sequence
from email import message_from_string
from pathlib import Path

_VERSION_HEADER = "Version"
_WHEEL_GLOB = "*.whl"
_SDIST_GLOB = "*.tar.gz"
_WHEEL_METADATA_SUFFIX = ".dist-info/METADATA"
_SDIST_PKG_INFO_NAME = "PKG-INFO"


class ReleaseVersionError(Exception):
    """Raised for any fail-closed release-version condition."""


def _version_from_message(raw: str, *, source: str) -> str:
    """Return the non-empty ``Version:`` header from raw RFC-822 metadata text.

    Reads the header case-insensitively via ``email.message_from_string`` and
    raises :class:`ReleaseVersionError` when it is missing or blank.
    """
    message = message_from_string(raw)
    version = message.get(_VERSION_HEADER)
    if version is None or not version.strip():
        raise ReleaseVersionError(f"Missing '{_VERSION_HEADER}:' header in {source}")
    return version.strip()


def _read_wheel_version(wheel: Path) -> str:
    """Return the version from the single ``*.dist-info/METADATA`` in the wheel.

    Fails closed unless the wheel archive contains exactly one member whose
    path ends with ``.dist-info/METADATA``.
    """
    with zipfile.ZipFile(wheel) as archive:
        members = [
            name
            for name in archive.namelist()
            if name.endswith(_WHEEL_METADATA_SUFFIX)
        ]
        if len(members) != 1:
            raise ReleaseVersionError(
                f"Expected exactly one '*{_WHEEL_METADATA_SUFFIX}' member in "
                f"{wheel.name}, found {len(members)}"
            )
        raw = archive.read(members[0]).decode("utf-8")

    return _version_from_message(raw, source=f"{wheel.name}:{members[0]}")


def _read_sdist_version(sdist: Path) -> str:
    """Return the version from the single TOP-LEVEL ``PKG-INFO`` in the sdist.

    The real sdist contains TWO ``PKG-INFO`` members: the top-level
    ``<pkg>-<ver>/PKG-INFO`` and a nested
    ``<pkg>-<ver>/src/<pkg>.egg-info/PKG-INFO``. Only the top-level member (whose
    parent path contains no ``/`` separator) is authoritative; fails closed
    unless exactly one such member exists.
    """
    with tarfile.open(sdist, "r:gz") as archive:
        top_level_members = [
            member.name
            for member in archive.getmembers()
            if member.isfile()
            and member.name.rsplit("/", 1)[-1] == _SDIST_PKG_INFO_NAME
            and "/" not in member.name.rsplit("/", 1)[0]
        ]
        if len(top_level_members) != 1:
            raise ReleaseVersionError(
                f"Expected exactly one top-level '{_SDIST_PKG_INFO_NAME}' member "
                f"in {sdist.name}, found {len(top_level_members)}"
            )
        extracted = archive.extractfile(top_level_members[0])
        if extracted is None:
            raise ReleaseVersionError(
                f"Could not read top-level '{_SDIST_PKG_INFO_NAME}' from {sdist.name}"
            )
        raw = extracted.read().decode("utf-8")

    return _version_from_message(raw, source=f"{sdist.name}:{top_level_members[0]}")


def collect_dist_versions(dist_dir: Path) -> tuple[str, str]:
    """Return ``(wheel_version, sdist_version)`` for the single artifact pair.

    Fails closed unless ``dist_dir`` contains exactly one wheel and exactly one
    sdist.
    """
    wheels = sorted(dist_dir.glob(_WHEEL_GLOB))
    sdists = sorted(dist_dir.glob(_SDIST_GLOB))

    if len(wheels) != 1:
        raise ReleaseVersionError(
            f"Expected exactly one wheel (*{_WHEEL_GLOB}) in {dist_dir}, "
            f"found {len(wheels)}"
        )
    if len(sdists) != 1:
        raise ReleaseVersionError(
            f"Expected exactly one sdist (*{_SDIST_GLOB}) in {dist_dir}, "
            f"found {len(sdists)}"
        )

    return _read_wheel_version(wheels[0]), _read_sdist_version(sdists[0])


def check_release_version(dist_dir: Path, tag: str) -> str:
    """Return the agreed version; fail closed on inconsistency or tag mismatch.

    ``tag`` may carry a single leading ``v`` (stripped here). The comparison is
    an EXACT string equality against the version shared by the wheel and sdist;
    no PEP 440 normalization is applied.
    """
    expected = tag.removeprefix("v")
    if not expected:
        raise ReleaseVersionError("Tag is empty after stripping a leading 'v'")

    wheel_version, sdist_version = collect_dist_versions(dist_dir)
    if wheel_version != sdist_version:
        raise ReleaseVersionError(
            "Wheel/sdist version mismatch: "
            f"wheel has {wheel_version!r}, sdist has {sdist_version!r}"
        )
    if expected != wheel_version:
        raise ReleaseVersionError(
            f"Tag {tag!r} (expected version {expected!r}) does not match "
            f"artifact version {wheel_version!r}"
        )

    return wheel_version


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; return 0 on success, 1 on any ``ReleaseVersionError``."""
    parser = argparse.ArgumentParser(
        description="Verify the built artifact version matches the release tag."
    )
    parser.add_argument(
        "--tag",
        required=True,
        help="Release tag (a single leading 'v' is stripped before comparison).",
    )
    parser.add_argument(
        "--dist-dir",
        default="dist",
        type=Path,
        help="Directory containing the built wheel and sdist (default: dist).",
    )
    args = parser.parse_args(argv)

    try:
        version = check_release_version(args.dist_dir, args.tag)
    except ReleaseVersionError as error:
        print(f"Release version check failed: {error}", file=sys.stderr)
        return 1

    print(f"Release version check passed: {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
