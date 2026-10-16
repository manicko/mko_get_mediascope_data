class NoReportFoundError(Exception):
    pass


class WrongDataSettings(Exception):
    """Raised when report data settings cannot be resolved.

    Reserved for future use in ``merge_data_settings`` to provide context
    when a subtype's default values are missing or inconsistent.  Currently
    unused — subtype defaults are covered by the bundled YAML templates
    (see ``settings/defaults/``).
    """
