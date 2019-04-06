

from tendril.utils.config import ConfigOption
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = []


config_elements_db = [
    ConfigOption(
        'DATABASE_HOST',
        "None",
        "The database server host."
    ),
    ConfigOption(
        'DATABASE_PORT',
        "5432",
        "The database server port."
    ),
    ConfigOption(
        'DATABASE_USER',
        "None",
        "The username to login to the database server."
    ),
    ConfigOption(
        'DATABASE_PASS',
        "None",
        "The password to login to the database server."
    ),
    ConfigOption(
        'DATABASE_DB',
        "None",
        "The name of the database."
    ),
]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    manager.load_elements(config_elements_db,
                          doc="Database Configuration")
