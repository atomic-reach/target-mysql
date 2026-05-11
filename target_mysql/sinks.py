"""MySQL target sink class, which handles writing streams."""

from __future__ import annotations

import atexit
import json
import logging
import os
import re
import string
import tempfile
import time
import typing as t
from typing import Any, Dict, Iterable, List, Optional, cast

import sqlalchemy
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Engine, URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import PrimaryKeyConstraint

if t.TYPE_CHECKING:
    from sqlalchemy.engine.reflection import Inspector


# Module-level cache so cert tempfiles outlive any single connector
# instance and get cleaned up at process exit. Singer targets are one-shot
# subprocesses, so atexit teardown is sufficient.
_CERT_TEMPFILES: list[str] = []


def _atexit_cleanup_certs() -> None:
    for path in _CERT_TEMPFILES:
        try:
            os.unlink(path)
        except OSError:
            pass


atexit.register(_atexit_cleanup_certs)


def _materialize_pem_if_inline(value: str | None, hint: str) -> str | None:
    """Return a filesystem path to PEM content.

    Accepts either a filesystem path (returned as-is) or inline PEM content
    starting with `-----BEGIN ` (written to a tempfile and the path returned).
    Tempfiles are tracked module-globally and unlinked at process exit.
    """
    if not value:
        return None
    text = str(value)
    if text.startswith("-----BEGIN "):
        tf = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".pem",
            prefix=f"target-mysql-{hint}-",
            delete=False,
            encoding="utf-8",
        )
        try:
            tf.write(text)
        finally:
            tf.close()
        _CERT_TEMPFILES.append(tf.name)
        return tf.name
    return text  # Treat as path (existence check happens at connect time).


class MySQLConnector(SQLConnector):
    """The connector for MySQL.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.
    table_name_pattern: str = "${TABLE_NAME}"  # The pattern to use for temp table names.

    # SSH tunnel forwarder kept on the connector instance so it lives as long
    # as the SQLAlchemy engine. Torn down implicitly at process exit.
    _ssh_tunnel = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.logger.setLevel(logging.DEBUG)

        self.allow_column_alter = super().config.get("allow_column_alter", False)

    # ------------------------------------------------------------------
    # SSH tunnel
    # ------------------------------------------------------------------

    def _maybe_open_ssh_tunnel(self, config: dict) -> tuple[str, int] | None:
        """Open an SSH tunnel if `ssh_tunnel.enable` is set.

        Returns (local_host, local_port) for the forwarded MySQL endpoint,
        or None if no tunnel is required. The forwarder is held on
        `self._ssh_tunnel` so it stays alive for the engine's lifetime.
        """
        ssh = config.get("ssh_tunnel") or {}
        if not ssh.get("enable"):
            return None
        if self._ssh_tunnel is not None:
            # Already opened (idempotent on repeated get_sqlalchemy_url calls).
            local = self._ssh_tunnel.local_bind_address
            return local[0], local[1]

        from sshtunnel import SSHTunnelForwarder

        remote_host = config["host"]
        remote_port = int(config.get("port", 3306))

        pkey_path = _materialize_pem_if_inline(ssh.get("private_key"), "ssh-key")
        ssh_password = ssh.get("password")

        # paramiko (under sshtunnel) accepts both ssh_pkey and ssh_password
        # at the same time; if a key is supplied it tries that first and
        # falls back to password auth on failure. Real-world configs use
        # one or the other; we forward both so callers can choose.
        forwarder = SSHTunnelForwarder(
            (ssh["host"], int(ssh.get("port", 22))),
            ssh_username=ssh["username"],
            ssh_pkey=pkey_path,
            ssh_private_key_password=ssh.get("private_key_password"),
            ssh_password=ssh_password,
            remote_bind_address=(remote_host, remote_port),
            # local_bind_address auto-picks an ephemeral port on 127.0.0.1
        )
        forwarder.start()
        self._ssh_tunnel = forwarder
        self.logger.info(
            "SSH tunnel established: %s:%d -> %s:%d via %s@%s:%d",
            forwarder.local_bind_address[0],
            forwarder.local_bind_address[1],
            remote_host,
            remote_port,
            ssh["username"],
            ssh["host"],
            int(ssh.get("port", 22)),
        )
        return forwarder.local_bind_address[0], forwarder.local_bind_address[1]

    # ------------------------------------------------------------------
    # SSL connect_args
    # ------------------------------------------------------------------

    def _build_ssl_connect_args(self, config: dict) -> dict:
        """Translate ssl_mode + ssl_ca/cert/key into PyMySQL connect_args.

        PyMySQL accepts an `ssl` dict with `ca`, `cert`, `key`, `cipher` (all
        file paths) and `check_hostname` (bool). To force TLS without cert
        verification we still pass an `ssl` dict (empty is fine) — PyMySQL
        takes the presence of `ssl` as the signal to negotiate TLS.
        """
        ssl_mode = config.get("ssl_mode")
        if not ssl_mode or ssl_mode == "disabled":
            return {}

        ssl_dict: dict[str, t.Any] = {}

        ca = _materialize_pem_if_inline(config.get("ssl_ca"), "ssl-ca")
        cert = _materialize_pem_if_inline(config.get("ssl_cert"), "ssl-cert")
        key = _materialize_pem_if_inline(config.get("ssl_key"), "ssl-key")
        cipher = config.get("ssl_cipher")
        if ca:
            ssl_dict["ca"] = ca
        if cert:
            ssl_dict["cert"] = cert
        if key:
            ssl_dict["key"] = key
        if cipher:
            ssl_dict["cipher"] = cipher

        if ssl_mode == "verify_identity":
            ssl_dict["check_hostname"] = True
        elif ssl_mode == "verify_ca":
            ssl_dict["check_hostname"] = False
        # 'preferred' / 'required' use whatever's in ssl_dict without forcing
        # cert verification. PyMySQL treats `ssl={}` as "negotiate TLS, accept
        # any cert" which matches `required` semantics.

        return {"ssl": ssl_dict}

    # ------------------------------------------------------------------
    # SQLAlchemy URL + engine
    # ------------------------------------------------------------------

    def get_sqlalchemy_url(self, config: dict) -> URL:
        """Generates a SQLAlchemy URL for MySQL.

        If `ssh_tunnel.enable` is set, the URL is rewritten to point at the
        local end of the forwarder so SQLAlchemy connects through the tunnel.
        This applies whether the URL is built from discrete host/port/user
        fields or supplied verbatim via `sqlalchemy_url` — in the latter
        case we parse the URL, open the tunnel, and replace the URL's
        `host`/`port` with the local bind address. Query params (e.g.
        PyMySQL SSL flags) are preserved.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            url = sqlalchemy.engine.url.make_url(config["sqlalchemy_url"])
            tunnel = self._maybe_open_ssh_tunnel(
                {**config, "host": url.host, "port": url.port or 3306}
            )
            if tunnel is not None:
                url = url.set(host=tunnel[0], port=tunnel[1])
            return url

        host = config["host"]
        port = config.get("port", "3306")
        tunnel = self._maybe_open_ssh_tunnel(config)
        if tunnel is not None:
            host, port = tunnel

        # Default to PyMySQL; user can override via driver_name to fall back
        # to mysqlclient (`mysql`).
        drivername = config.get("driver_name", "mysql+pymysql")

        return sqlalchemy.engine.url.URL.create(
            drivername=drivername,
            username=config.get("user") or config.get("username"),
            password=config["password"],
            host=host,
            port=port,
            database=config["database"],
        )

    def create_engine(self) -> Engine:
        """Override to inject SSL connect_args from config."""
        url = self.sqlalchemy_url
        connect_args = self._build_ssl_connect_args(self.config)
        return sqlalchemy.create_engine(url, connect_args=connect_args, echo=False)

    def get_fully_qualified_name(
            self,
            table_name: str | None = None,
            schema_name: str | None = None,
            db_name: str | None = None,
            delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If all 3 name parts not supplied.

        Returns:
            The fully qualified name as a string.
        """
        table_name_pattern = self.config.get("table_name_pattern")
        table_name_pattern = string.Template(table_name_pattern).substitute({"TABLE_NAME": table_name})
        if table_name_pattern == "" or table_name_pattern is None:
            table_name_pattern = table_name

        parts = []
        if db_name:
            parts.append(db_name)
        if schema_name:
            parts.append(schema_name)
        if table_name:
            parts.append(table_name_pattern)

        if not parts:
            raise ValueError(
                "Could not generate fully qualified name: "
                + ":".join(
                    [
                        db_name or "(unknown-db)",
                        schema_name or "(unknown-schema)",
                        table_name or "(unknown-table-name)",
                    ],
                ),
            )

        return delimiter.join(parts)

    def get_object_names(
            self,
            engine: Engine,  # noqa: ARG002
            inspected: Inspector,
            schema_name: str,
    ) -> list[tuple[str, bool]]:
        """Return a list of syncable objects.
        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect

        Returns:
            List of tuples (<table_or_view_name>, <is_view>)
        """
        # Get list of tables and views
        table_names = inspected.get_table_names(schema=schema_name)
        try:
            view_names = inspected.get_view_names(schema=schema_name)
        except NotImplementedError:
            # Some DB providers do not understand 'views'
            self._warn_no_view_detection()
            view_names = []

        objects = [(t, False) for t in table_names] + [(v, True) for v in view_names]
        if self.config.get("lower_case_table_names", True):
            objects = [x.lower() for x in objects]

        return objects


    def prepare_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
        """
        if not self.column_exists(full_table_name, column_name):
            self._create_empty_column(
                full_table_name=full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )
            return

        if not self.config.get('freeze_schema'):
            self._adapt_column_type(
                full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )


    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:  # noqa
        """Convert JSON Schema type to a SQL type.
        Args:
            jsonschema_type: The JSON Schema object.
        Returns:
            The SQL type.
        """
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(
                        sqlalchemy.types.TypeEngine, mysql.DATETIME()
                    )
                elif datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, mysql.TIME())
                elif datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, mysql.DATE())
                elif datelike_type == "binary":
                    return cast(sqlalchemy.types.TypeEngine, mysql.BINARY())

            # The maximum row size for the used table type, not counting BLOBs, is 65535.
            #
            # `default_string_length` (config, default 255) controls the
            # VARCHAR length used when Singer schema doesn't specify
            # `maxLength`. Defaulting to 1000 (the 0.1.x behaviour) breaks
            # under InnoDB's 3072-byte index limit on default MySQL 8
            # (utf8mb4 = 4 bytes/char): VARCHAR(1000) PK columns fail with
            # `(1071, 'Specified key was too long')`. 255 fits comfortably
            # under any modern MySQL InnoDB index limit.
            default_length = int(self.config.get("default_string_length", 255))
            maxlength = jsonschema_type.get("maxLength", default_length)
            data_type = mysql.VARCHAR(maxlength)
            if maxlength <= 1000:
                return cast(sqlalchemy.types.TypeEngine, mysql.VARCHAR(maxlength))
            elif maxlength <= 65535:
                return cast(sqlalchemy.types.TypeEngine, mysql.TEXT(maxlength))
            elif maxlength <= 16777215:
                return cast(sqlalchemy.types.TypeEngine, mysql.MEDIUMTEXT())
            elif maxlength <= 4294967295:
                return cast(sqlalchemy.types.TypeEngine, mysql.LONGTEXT())

            return cast(sqlalchemy.types.TypeEngine, data_type)

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            minimum = jsonschema_type.get("minimum", -9223372036854775807)
            maximum = jsonschema_type.get("maximum", 9223372036854775807)

            if minimum >= -128 and maximum <= 127:
                return cast(sqlalchemy.types.TypeEngine, mysql.TINYINT(unsigned=False))
            elif minimum >= -32768 and maximum <= 32767:
                return cast(sqlalchemy.types.TypeEngine, mysql.SMALLINT(unsigned=False))
            elif minimum >= -8388608 and maximum <= 8388607:
                return cast(sqlalchemy.types.TypeEngine, mysql.MEDIUMINT(unsigned=False))
            elif minimum >= -2147483648 and maximum <= 2147483647:
                return cast(sqlalchemy.types.TypeEngine, mysql.INTEGER(unsigned=False))
            elif minimum >= -9223372036854775808 and maximum <= 9223372036854775807:
                return cast(sqlalchemy.types.TypeEngine, mysql.BIGINT(unsigned=False))
            elif minimum >= 0 and maximum <= 255:
                return cast(sqlalchemy.types.TypeEngine, mysql.TINYINT(unsigned=True))
            elif minimum >= 0 and maximum <= 65535:
                return cast(sqlalchemy.types.TypeEngine, mysql.SMALLINT(unsigned=True))
            elif minimum >= 0 and maximum <= 16777215:
                return cast(sqlalchemy.types.TypeEngine, mysql.MEDIUMINT(unsigned=True))
            elif minimum >= 0 and maximum <= 4294967295:
                return cast(sqlalchemy.types.TypeEngine, mysql.INTEGER(unsigned=True))
            elif minimum >= 0 and maximum <= 18446744073709551615:
                return cast(sqlalchemy.types.TypeEngine, mysql.BIGINT(unsigned=True))

        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            if 'multipleOf' in jsonschema_type:
                return cast(sqlalchemy.types.TypeEngine, mysql.DECIMAL())
            else:
                return cast(sqlalchemy.types.TypeEngine, mysql.FLOAT())

        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, mysql.BOOLEAN())

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            # if 'format' in jsonschema_type and jsonschema_type.get("format") == "spatial":
            #     return cast(sqlalchemy.types.TypeEngine, mysql.MU)
            return cast(sqlalchemy.types.TypeEngine, mysql.JSON())

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TEXT(4000))

        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TEXT(4000))

    def _jsonschema_type_check(
            self, jsonschema_type: dict, type_check: tuple[str]
    ) -> bool:
        """Return True if the jsonschema_type supports the provided type.
        Args:
            jsonschema_type: The type dict.
            type_check: A tuple of type strings to look for.
        Returns:
            True if the schema suports the type.
        """
        if "type" in jsonschema_type:
            if isinstance(jsonschema_type["type"], (list, tuple)):
                for t in jsonschema_type["type"]:
                    if t in type_check:
                        return True
            else:
                if jsonschema_type.get("type") in type_check:
                    return True

        if any(t in type_check for t in jsonschema_type.get("anyOf", ())):
            return True

        return False

    def _create_empty_column(
            self,
            full_table_name: str,
            column_name: str,
            sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.
        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            # raise NotImplementedError("Adding columns is not supported.")
            return

        # if column_name.startswith("_"):
        #     column_name = f"x{column_name}"

        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                sql_type,
                quote=False
            )
        )

        try:
            alter_sql = f"""ALTER TABLE {str(full_table_name)}
                ADD COLUMN {str(create_column_clause)} """
            self.logger.info("Altering with SQL: %s", alter_sql)
            self.connection.execute(alter_sql)
        except Exception as e:
            raise RuntimeError(
                f"Could not create column '{create_column_clause}' "
                f"on table '{full_table_name}'."
            ) from e

    # def create_temp_table_from_table(self, from_table_name, temp_table_name):
    #     """Temp table from another table."""
    #
    #     try:
    #         self.connection.execute(
    #             f"""DROP TABLE {temp_table_name}"""
    #         )
    #     except Exception as e:
    #         pass
    #
    #     ddl = f"""
    #         CREATE TABLE {temp_table_name} AS (
    #             SELECT * FROM {from_table_name}
    #             WHERE 1=0
    #         )
    #     """
    #
    #     self.connection.execute(ddl)

    def create_empty_table(
            self,
            full_table_name: str,
            schema: dict,
            primary_keys: list[str] | None = None,
            partition_keys: list[str] | None = None,
            as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(schema=schema_name)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
            )

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sqlalchemy.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema)
                )
            )

        if primary_keys:
            pk_constraint = PrimaryKeyConstraint(*primary_keys, name=f"{table_name}_PK")
            _ = sqlalchemy.Table(table_name, meta, *columns, pk_constraint)
        else:
            _ = sqlalchemy.Table(table_name, meta, *columns)

        try:
            meta.create_all(self._engine)
        except OperationalError as e:
            # Translate the cryptic `(1071, 'Specified key was too long')`
            # into something operators can act on. Triggered when a string
            # column declared as VARCHAR exceeds InnoDB's 3072-byte index
            # limit — typically because a primary-key column was sized at
            # the legacy default of VARCHAR(1000) under utf8mb4.
            if "1071" in str(e) or "Specified key was too long" in str(e):
                pk_cols = ", ".join(primary_keys) if primary_keys else "(none)"
                raise RuntimeError(
                    "MySQL refused to create the primary-key index — "
                    "the key length exceeds InnoDB's 3072-byte limit. This "
                    "usually means a VARCHAR primary-key column is too wide "
                    "for the database's charset (utf8mb4 = 4 bytes/char). "
                    f"Primary keys on '{table_name}': {pk_cols}. Lower the "
                    "`default_string_length` config (current default 255), "
                    "or set `maxLength` on the offending Singer property, "
                    "or switch the database to utf8mb3.\n"
                    f"Underlying error: {e}"
                ) from e
            raise

    def merge_sql_types(  # noqa
            self, sql_types: list[sqlalchemy.types.TypeEngine]
    ) -> sqlalchemy.types.TypeEngine:  # noqa
        """Return a compatible SQL type for the selected type list.
        Args:
            sql_types: List of SQL types.
        Returns:
            A SQL type that is compatible with the input types.
        Raises:
            ValueError: If sql_types argument has zero members.
        """
        if not sql_types:
            raise ValueError("Expected at least one member in `sql_types` argument.")

        if len(sql_types) == 1:
            return sql_types[0]

        # Gathering Type to match variables
        # sent in _adapt_column_type
        current_type = sql_types[0]
        # sql_type = sql_types[1]

        # Getting the length of each type
        # current_type_len: int = getattr(sql_types[0], "length", 0)
        sql_type_len: int = getattr(sql_types[1], "length", 0)
        if sql_type_len is None:
            sql_type_len = 0

        # Convert the two types given into a sorted list
        # containing the best conversion classes
        sql_types = self._sort_types(sql_types)

        # If greater than two evaluate the first pair then on down the line
        if len(sql_types) > 2:
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:]
            )

        assert len(sql_types) == 2
        # Get the generic type class
        for opt in sql_types:
            # Get the length
            opt_len: int = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            current_type_length = 0
            if isinstance(current_type, sqlalchemy.types.TEXT) and current_type.length is None:
                current_type_length = 65535
            elif hasattr(current_type, 'length'):
                current_type_length = current_type.length

            if isinstance(generic_type, type):
                if issubclass(
                        generic_type,
                        (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                            (opt_len is None)
                            or (opt_len == 0)
                            or (opt_len >= current_type_length)
                    ):
                        return opt
                elif isinstance(
                        generic_type,
                        (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                            (opt_len is None)
                            or (current_type is None)
                            or (opt_len == 0)
                            or (opt_len >= current_type_length)
                    ):
                        return opt
                # If best conversion class is equal to current type
                # return the best conversion class
                elif str(opt) == str(current_type):
                    return opt

        raise ValueError(
            f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}"
        )

    def _adapt_column_type(
            self,
            full_table_name: str,
            column_name: str,
            sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.
        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name, column_name
        )

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type).split(" ")[0] == str(current_type).split(" ")[0]:
            # Nothing to do
            return

        if self.allow_column_alter:
            try:
                alter_sql = f"""ALTER TABLE {str(full_table_name)}
                    MODIFY {str(column_name)} {str(compatible_sql_type)}"""
                self.logger.info("Altering with SQL: %s", alter_sql)
                self.connection.execute(alter_sql)
            except Exception as e:
                raise RuntimeError(
                    f"Could not convert column '{full_table_name}.{column_name}' "
                    f"from '{current_type}' to '{compatible_sql_type}'."
                ) from e


class MySQLSink(SQLSink):
    """MySQL target sink class."""

    MAX_SIZE_DEFAULT = 10000

    connector_class = MySQLConnector

    soft_delete_column_name = "x_sdc_deleted_at"
    version_column_name = "x_sdc_table_version"

    start_time_global = time.time()
    inserted_records = 0

    # @property
    # def schema_name(self) -> Optional[str]:
    #     """Return the schema name or `None` if using names with no schema part.
    #     Returns:
    #         The target schema name.
    #     """
    #     # Look for a default_target_scheme in the configuraion fle
    #     default_target_schema: str = self.config.get("default_target_schema", None)
    #     parts = self.stream_name.split("-")

    #     # 1) When default_target_scheme is in the configuration use it
    #     # 2) if the streams are in <schema>-<table> format use the
    #     #    stream <schema>
    #     # 3) Return None if you don't find anything
    #     if default_target_schema:
    #         return default_target_schema

    #     # Schema name not detected.
    #     return None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.logger.setLevel(logging.DEBUG)

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        # First we need to be sure the main table is already created
        conformed_records = (
            [self.conform_record(record) for record in context["records"]]
            if isinstance(context["records"], list)
            else (self.conform_record(record) for record in context["records"])
        )

        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

        self.bulk_insert_records(
            full_table_name=self.full_table_name,
            schema=schema,
            records=conformed_records,
        )

        # if self.key_properties:
        #     self.logger.info(f"Preparing table {self.full_table_name}")
        #     self.connector.prepare_table(
        #         full_table_name=self.full_table_name,
        #         schema=schema,
        #         primary_keys=self.key_properties,
        #         as_temp_table=False,
        #     )
        #
        #     tmp_table_name = self.full_table_name + "_temp"
        #
        #     # Create a temp table (Creates from the table above)
        #     self.logger.info(f"Creating temp table {self.full_table_name}")
        #     self._connector.create_temp_table_from_table(
        #         from_table_name=self.full_table_name,
        #         temp_table_name=tmp_table_name
        #     )
        #
        #     # Insert into temp table
        #     self.bulk_insert_records(
        #         full_table_name=tmp_table_name,
        #         schema=schema,
        #         records=conformed_records,
        #     )
        #     # Merge data from Temp table to main table
        #     self.logger.info(f"Merging data from temp table to {self.full_table_name}")
        #     self.merge_upsert_from_table(
        #         from_table_name=tmp_table_name,
        #         to_table_name=self.full_table_name,
        #         join_keys=join_keys,
        #     )
        #
        # else:
        #     self.bulk_insert_records(
        #         full_table_name=self.full_table_name,
        #         schema=schema,
        #         records=conformed_records,
        #     )

    # def merge_upsert_from_table(self,
    #                             from_table_name: str,
    #                             to_table_name: str,
    #                             join_keys: List[str],
    #                             ) -> Optional[int]:
    #
    #     """Merge upsert data from one table to another.
    #     Args:
    #         from_table_name: The source table name.
    #         to_table_name: The destination table name.
    #         join_keys: The merge upsert keys, or `None` to append.
    #         schema: Singer Schema message.
    #     Return:
    #         The number of records copied, if detectable, or `None` if the API does not
    #         report number of records affected/inserted.
    #     """
    #     # TODO think about sql injeciton,
    #     # issue here https://github.com/MeltanoLabs/target-postgres/issues/22
    #
    #     join_keys = [self.conform_name(key, "column") for key in join_keys]
    #     schema = self.conform_schema(self.schema)
    #
    #     join_condition = " and ".join(
    #         [f"temp.{key} = target.{key}" for key in join_keys]
    #     )
    #
    #     upsert_on_condition = ", ".join(
    #         [f"{to_table_name}.{key} = temp.{key}" for key in join_keys]
    #     )
    #
    #     merge_sql = f"""
    #         INSERT INTO {to_table_name} ({", ".join(schema["properties"].keys())})
    #             SELECT {", ".join(schema["properties"].keys())}
    #             FROM
    #                 {from_table_name} temp
    #         ON DUPLICATE KEY UPDATE
    #             {upsert_on_condition}
    #     """
    #
    #     self.logger.debug("Merging with SQL: %s", merge_sql)
    #
    #     self.connection.execute(merge_sql)
    #
    #     self.connection.execute("COMMIT")
    #
    #     self.connection.execute(f"DROP TABLE {from_table_name}")
    #
    #     self.logger.info(f"Dropped temp table '{from_table_name}'")

    def bulk_insert_records(
            self,
            full_table_name: str,
            schema: dict,
            records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.
        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )
        if self.key_properties:
            join_keys = [self.conform_name(key, "column") for key in self.key_properties]
            upsert_on_condition = ", ".join(
                [f"{key}=VALUES({key})" for key in join_keys]
            )
            insert_sql += f" ON DUPLICATE KEY UPDATE {upsert_on_condition}"

        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        self.logger.debug("Inserting with SQL: %s", insert_sql)

        columns = self.column_representation(schema)

        # temporary fix to ensure missing properties are added
        insert_records = []

        for record in records:
            insert_record = {}
            conformed_record = self.conform_record(record)
            for column in columns:
                # insert_record[column.name] = conformed_record.get(column.name)

                val = conformed_record.get(column.name)
                if (isinstance(val, Dict) or isinstance(val, List)):
                    val = json.dumps(val)

                insert_record[column.name] = val
            insert_records.append(insert_record)

        self.connection.execute(insert_sql, insert_records)
        self.connection.execute("COMMIT")

        if isinstance(records, list):
            self.inserted_records += len(records)
            elapsed_time_global = time.time() - self.start_time_global
            avg_per_minute = (self.inserted_records / elapsed_time_global) * 60

            self.logger.info(f"Table '{full_table_name}'")
            self.logger.info(f"  - Total inserted records: {format(int(self.inserted_records), ',')} ")
            self.logger.info(f"  - Total time elapsed: {self.format_time(elapsed_time_global)}")
            self.logger.info(f"  - Average processed per minute: {format(int(avg_per_minute), ',')}")

            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.

    def column_representation(
            self,
            schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    def snakecase(self, name):
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    def move_leading_underscores(self, text):
        match = re.match(r'^(_*)(.*)', text)
        if match:
            result = match.group(2) + match.group(1)
            return result
        return text

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conform a stream property name to one suitable for the target system.
        Transforms names to snake case by default, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.
        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.
        Returns:
            The name transformed to snake case.
        """
        # strip non-alphanumeric characters except _.
        name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)

        if super().config.get("move_leading_underscores", True):
            # Move leading underscores to the end of the name
            name = self.move_leading_underscores(name)

        if super().config.get("snakecase_names", True):
            # convert to snakecase
            name = self.snakecase(name)

        if super().config.get("replace_leading_digit", True):
            # replace leading digit
            name = replace_leading_digit(name)

        return name

    def format_time(self, elapsed_time):
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"
