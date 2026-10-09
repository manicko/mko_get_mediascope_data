import logging

import mko_get_medioscope_data.core.network as n
import mko_get_medioscope_data.core.services as s
import mko_get_medioscope_data.core.reports as r
import mko_get_medioscope_data.core.tasks as t
import mko_get_medioscope_data.core.utils as u

mods = [n, s, r, t, u]

print("=== actual module logger names (logging.getLogger(__name__)) ===")
for m in mods:
    print("  " + m.logger.name)

cfg = s.app_service.app_config.logging_settings
print("=== loggers DECLARED in log_config.yaml ===")
for k in cfg.loggers.keys():
    print("  " + k)

print("=== root logger handlers (after import-time dictConfig) ===")
root = logging.getLogger()
for h in root.handlers:
    dest = getattr(h, "baseFilename", None) or getattr(h, "stream", None)
    print("  " + type(h).__name__ + " -> " + str(dest))

print("=== app_config handlers (rotating_file filename) ===")
print("  rotating_file.filename =", cfg.handlers["rotating_file"].get("filename"))
