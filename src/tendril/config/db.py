

from tendril.utils.config import ConfigOption
from tendril.utils.config import ConfigOptionConstruct
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = ['tendril.config.core',
           'tendril.config.db_core']


def build_db_uri(dbhost, dbport, dbuser, dbpass, dbname):
    """
    Builds a ``postgresql`` DB URI from the parameters provided.

    :param dbhost: Hostname / IP of the database server
    :param dbport: Port of the database server
    :param dbuser: Username of the database user
    :param dbpass: Password of the database user
    :param dbname: Name of the database
    :return: The DB URI
    """
    return 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
        dbuser, dbpass, dbhost, dbport, dbname
    )


class DbUri(ConfigOptionConstruct):
    @property
    def value(self):
        return build_db_uri(
            dbhost=self.ctx["DATABASE{}_HOST".format(self._parameters)],
            dbport=self.ctx["DATABASE{}_PORT".format(self._parameters)],
            dbuser=self.ctx["DATABASE{}_USER".format(self._parameters)],
            dbpass=self.ctx["DATABASE{}_PASS".format(self._parameters)],
            dbname=self.ctx["DATABASE{}_DB".format(self._parameters)],
        )


def _db_config_template(db_code):
    return [
        ConfigOption(
            'DATABASE{}_HOST'.format(db_code),
            "None",
            "The database server host."
        ),
        ConfigOption(
            'DATABASE{}_PORT'.format(db_code),
            "5432",
            "The database server port."
        ),
        ConfigOption(
            'DATABASE{}_USER'.format(db_code),
            "None",
            "The username to login to the database server."
        ),
        ConfigOption(
            'DATABASE{}_PASS'.format(db_code),
            "None",
            "The password to login to the database server.",
            masked=True
        ),
        ConfigOption(
            'DATABASE{}_DB'.format(db_code),
            "None",
            "The name of the database."
        ),
        DbUri(
            'DATABASE{}_URI'.format(db_code),
            db_code,
            "Constructed Database URI string. This option is created by "
            "the code, and should not be set directly in any config file.",
        ),
        ConfigOption(
            'DATABASE{}_PACKAGE_PREFIXES'.format(db_code),
            "None",
            "List of package namespaces other than 'tendril' "
            "to search for DB models."
        )
    ]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    config_elements_db = []
    for code in manager.DB_SERVER_CODES:
        config_elements_db += _db_config_template(code)
    manager.load_elements(config_elements_db,
                          doc="Database Configuration")
