import logging.config
from mko_get_mediascope_data.core.config_service import CONFIG

logging.config.dictConfig(CONFIG.logging_settings)
