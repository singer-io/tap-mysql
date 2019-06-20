#!/usr/bin/env python3

import backoff

import pymysql
from pymysql.constants import CLIENT

import singer
import ssl

LOGGER = singer.get_logger()

CONNECT_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 3600

# We need to hold onto this for self-signed SSL
match_hostname = ssl.match_hostname

# MySQL 8.0 Patch:
# Workaround to support MySQL 8.0 without upgrading the PyMySQL version
# since there are breaking changes between these versions, this should suffice to allow
# new character sets to be used with MySQL 8.0 instances.
# FIXME: Remove when PyMYSQL upgrade behavior has been evaluated.
# Patch Originally Found Here: https://github.com/PyMySQL/PyMySQL/pull/592
original_charset_by_id = pymysql.charset.charset_by_id
def charset_wrapper(*args, **kwargs):
    unknown_charset = pymysql.charset.Charset(None, None, None, None)
    try:
        return original_charset_by_id(*args, **kwargs)
    except KeyError:
        return unknown_charset
pymysql.connections.charset_by_id = charset_wrapper

@backoff.on_exception(backoff.expo,
                      (pymysql.err.OperationalError),
                      max_tries=5,
                      factor=2)
def connect_with_backoff(connection):
    connection.connect()

    warnings = []
    with connection.cursor() as cur:
        try:
            cur.execute('SET @@session.time_zone="+0:00"')
        except pymysql.err.InternalError as e:
            warnings.append('Could not set session.time_zone. Error: ({}) {}'.format(*e.args))

        try:
            cur.execute('SET @@session.wait_timeout=2700')
        except pymysql.err.InternalError as e:
             warnings.append('Could not set session.wait_timeout. Error: ({}) {}'.format(*e.args))

        try:
            cur.execute("SET @@session.net_read_timeout={}".format(READ_TIMEOUT_SECONDS))
        except pymysql.err.InternalError as e:
             warnings.append('Could not set session.net_read_timeout. Error: ({}) {}'.format(*e.args))


        try:
            cur.execute('SET @@session.innodb_lock_wait_timeout=2700')
        except pymysql.err.InternalError as e:
            warnings.append(
                'Could not set session.innodb_lock_wait_timeout. Error: ({}) {}'.format(*e.args)
                )

        if warnings:
            LOGGER.info(("Encountered non-fatal errors when configuring MySQL session that could "
                         "impact performance:"))
        for w in warnings:
            LOGGER.warning(w)

    return connection


def parse_internal_hostname(hostname):
    # special handling for google cloud
    if ":" in hostname:
        parts = hostname.split(":")
        if len(parts) == 3:
            return parts[0] + ":" + parts[2]
        return parts[0] + ":" + parts[1]

    return hostname


class MySQLConnection(pymysql.connections.Connection):
    def __init__(self, config):
        # Google Cloud's SSL involves a self-signed certificate. This certificate's
        # hostname matches the form {instance}:{box}. The hostname displayed in the
        # Google Cloud UI is of the form {instance}:{region}:{box} which
        # necessitates the "parse_internal_hostname" function to get the correct
        # hostname to match.
        # The "internal_hostname" config variable allows for matching the SSL
        # against a host that doesn't match the host we are connecting to. In the
        # case of Google Cloud, we will be connecting to an IP, not the hostname
        # the SSL certificate expects.
        # The "ssl.match_hostname" function is patched to check against the
        # internal hostname rather than the host of the connection. In the event
        # that the connection fails, the patch is reverted by reassigning the
        # patched out method to it's original spot.

        args = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": int(config["port"]),
            "cursorclass": config.get("cursorclass") or pymysql.cursors.SSCursor,
            "connect_timeout": CONNECT_TIMEOUT_SECONDS,
            "read_timeout": READ_TIMEOUT_SECONDS,
            "charset": "utf8",
        }

        ssl_arg = None

        if config.get("database"):
            args["database"] = config["database"]

        use_ssl = config.get('ssl') == 'true'

        # Attempt self-signed SSL if config vars are present
        use_self_signed_ssl = config.get("ssl_ca")

        if use_ssl and use_self_signed_ssl:
            LOGGER.info("Using custom certificate authority")

            # Config values MUST be paths to files for the SSL module to read them correctly.
            ssl_arg = {
                "ca": config["ssl_ca"],
                "check_hostname": config.get("check_hostname", "true") == "true"
            }

            # If using client authentication, cert and key are required
            if config.get("ssl_cert") and config.get("ssl_key"):
                ssl_arg["cert"] = config["ssl_cert"]
                ssl_arg["key"] = config["ssl_key"]

            # override match hostname for google cloud
            if config.get("internal_hostname"):
                parsed_hostname = parse_internal_hostname(config["internal_hostname"])
                ssl.match_hostname = lambda cert, hostname: match_hostname(cert, parsed_hostname)

        super().__init__(defer_connect=True, ssl=ssl_arg, **args)

        # Configure SSL without custom CA
        # Manually create context to override default behavior of
        # CERT_NONE without a CA supplied
        if use_ssl and not use_self_signed_ssl:
            LOGGER.info("Attempting SSL connection")
            # For compatibility with previous version, verify mode is off by default
            verify_mode = config.get("verify_mode", "false") == 'true'
            if not verify_mode:
                LOGGER.warn("Not verifying server certificate. The connection is encrypted, but the server hasn't been verified. Please provide a root CA certificate to enable verification.")
            self.ssl = True
            self.ctx = ssl.create_default_context()
            check_hostname = config.get("check_hostname", "false") == 'true'
            self.ctx.check_hostname = check_hostname
            self.ctx.verify_mode = ssl.CERT_REQUIRED if verify_mode else ssl.CERT_NONE
            self.client_flag |= CLIENT.SSL


    def __enter__(self):
        return self


    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MySQLConnection):
        def __init__(self, *args, **kwargs):
            config["cursorclass"] = kwargs.get('cursorclass')
            super().__init__(config)

            connect_with_backoff(self)

    return ConnectionWrapper
