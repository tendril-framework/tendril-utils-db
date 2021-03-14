# Copyright (C) 2015 Chintalagiri Shashank
#
# This file is part of Tendril.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
The Database Utils Module (:mod:`tendril.utils.db`)
===================================================

This module provides utilities to deal with Tendril's Database. While the
actual functionality is provided by the :mod:`sqlalchemy` package, the
contents of this utility module simplify and specify the application code's
interaction with :mod:`sqlalchemy`

.. rubric:: Module Contents

"""

import importlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import Column, Integer
from sqlalchemy_utils import ArrowType

from contextlib import contextmanager
import functools
import arrow

from tendril.utils.versions import get_namespace_package_names

from tendril.config import DATABASE_HOST
from tendril.config import DATABASE_PORT
from tendril.config import DATABASE_USER
from tendril.config import DATABASE_PASS
from tendril.config import DATABASE_DB

from tendril.utils import log
logger = log.get_logger(__name__, log.INFO)
log.logging.getLogger('sqlalchemy.engine').setLevel(log.WARNING)

try:
    from tendril.devtooling import stack
except ImportError:
    stack = None


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


def init_db_engine():
    """
    Initializes the database engine and binds it to the Database URI
    defined by the :mod:`tendril.config` module.

    This function is called within the module and an engine is readily
    available in the module variable :data:`tendril.utils.db.engine`.
    Application code should not have to create a new engine for normal
    use cases.
    """
    return create_engine(
        build_db_uri(DATABASE_HOST, DATABASE_PORT,
                     DATABASE_USER, DATABASE_PASS,
                     DATABASE_DB)
    )


#: The :class:`sqlalchemy.Engine` object
engine = init_db_engine()

#: The :class:`sqlalchemy.sessionmaker` bound to the database engine
Session = sessionmaker(expire_on_commit=False)
Session.configure(bind=engine)


@contextmanager
def get_session():
    """
    Application executable code will typically only have to interact with this
    ``contextmanager`` or the :func:`with_db` decorator. It should use this to
    create a database session, perform its tasks, whatever they may be, within
    this context, and then exit the context.

    If any Exception is thrown, the session is rolled back completely. If no
    Exception is thrown or Exceptions are handled by the application code
    within the context, the session is committed when the context exits.

    .. seealso:: :func:`with_db`

    """
    if stack:
        logger.debug('Making session: {0}'.format(stack.get_caller(1)))
    session = Session()
    try:
        yield session
        session.commit()
    except:
        if stack:
            caller, ancestors = stack.get_caller(1, get_stack=True)
            logger.warning(
                "Rolling back session: {0}".format(str(caller))
            )
            logger.debug('ANCESTORS:')
            for frame in ancestors:
                logger.debug(stack.format_frame(frame[0]))

        session.rollback()
        raise
    finally:
        session.close()


def with_db(func):
    """
    Application executable code will typically only have to interact with this
    function or the :func:`get_session` ``contextmanager``. The
    :func:`with_db` decorator is intended to decorate functions which interact
    primarily with the db.

    Such a function would accept only keyword arguments, one of which is
    ``session``, which can be a database session (created by
    :func:`get_session`) provided by the caller. If ``session`` is ``None``,
    this decorator creates a new session and calls the decorated function
    using it.

    Any function which returns objects that still need to be bound to a db
    session should be called with a valid session, if you intend to do
    anything with the returned objects. They will still execute without
    exception if no session is provided, but the returned value may not be
    useful.

    .. seealso:: :func:`get_session`

    """
    @functools.wraps(func)
    def inner(session=None, **kwargs):
        if session is None:
            with get_session() as s:
                return func(session=s, **kwargs)
        else:
            return func(session=session, **kwargs)
    return inner


#: The :mod:`sqlalchemy` declarative base for all Models in Tendril
DeclBase = declarative_base()


class BaseMixin(object):
    """
    This Mixin can / should be used (by inheriting from) by all Model classes
    defined by application code. It defines the :attr:`__tablename__`
    attribute of the Model class to the name of the class and creates a
    Primary Key Column named id in the table for the Model.
    """
    @declared_attr
    def __tablename__(self):
        return self.__name__

    # __table_args__ = {'mysql_engine': 'InnoDB'}
    # __mapper_args__= {'always_refresh': True}

    id = Column(Integer, primary_key=True)


class CreatedTimestampMixin(object):
    """
    This Mixin can be used by any Models which require a creation timestamp
    to be created. It adds a column named ``created_at``, which defaults to
    the time at which the object is created.
    """
    created_at = Column(ArrowType, default=arrow.utcnow)


class UpdateTimestampMixin(object):
    """
    This Mixin can be used by any Models which require an update timestamp
    to be created. It adds a column named ``updated_at``, which defaults to
    the time at which the object is updated.
    """
    updated_at = Column(ArrowType, onupdate=arrow.utcnow)


class TimestampMixin(CreatedTimestampMixin, UpdateTimestampMixin):
    """
    This Mixin can be used for any Models which contain data that has time
    dependence to any degree. It adds both the ``updated_at`` and
    ``created_at`` columns.
    """
    pass


_excluded_prefixes = ['tendril.schema']


def get_metadata(prefix='tendril'):
    """
    This function populates the database metadata with all the models used
    by the application. The models are imported from <prefix>.*.db.models,
    where * represents a single package hierarchy. This is how database
    modules are distributed within tendril, and any application which wants
    to use this package should follow the same architecture and the correct
    prefix should be provided to this function.
    """
    for p in get_namespace_package_names(prefix):
        if p in _excluded_prefixes:
            continue
        try:
            modname = '{0}.db.model'.format(p)
            globals()[modname] = importlib.import_module(modname)
            logger.info("Loaded DB Models from {0}".format(p))
        except ImportError:
            pass

    return DeclBase.metadata


def commit_metadata():
    """
    This function commits all metadata to the database. This function should
    be run after importing **all** the Model classes, and it will create the
    tables in the database.
    """
    metadata.create_all(engine)


#: The full database/sqlalchemy metadata.
#:
#: This metadata will only be meaningful in an application, and not at the
#: db utils package level. The application can import this metadata or
#: simply add the metadata to its documentation identically as here to get
#: a complete ER diagram.
#:
#: Rendered by :mod:`sqlalchemyviz` into the following ER diagram.
#:
#: .. sqlaviz::
#:     :metadataobject: tendril.utils.db.metadata
metadata = get_metadata()
