"""MySQL target class."""

from __future__ import annotations

import io
import simplejson as json

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget
import typing as t

from target_mysql.sinks import (
    MySQLSink,
)


class TargetMySQL(SQLTarget):
    """Singer target for MySQL."""

    name = "target-mysql"

    default_sink_class = MySQLSink

    config_jsonschema = th.PropertiesList(
        # ----- Connection: full URL override (advanced) -----
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description=(
                "SQLAlchemy connection string. When set, overrides the discrete "
                "host/port/user/password/database fields. Use this for "
                "custom drivers (e.g. mysql+pymysql://) or to embed PyMySQL "
                "URL params directly."
            ),
        ),
        th.Property(
            "driver_name",
            th.StringType,
            default="mysql+pymysql",
            description=(
                "SQLAlchemy driver name. Defaults to 'mysql+pymysql' (pure-Python, "
                "no system build deps). Set to 'mysql' to use the mysqlclient C "
                "extension instead — requires `pip install mysqlclient` and "
                "system libmysqlclient/dev headers."
            ),
        ),

        # ----- Connection: discrete fields -----
        th.Property("username", th.StringType, secret=True, description="MySQL username"),
        th.Property("password", th.StringType, secret=True, description="MySQL password"),
        th.Property("host", th.StringType, description="MySQL host"),
        th.Property("port", th.StringType, default="3306", description="MySQL port"),
        th.Property("database", th.StringType, description="MySQL database"),

        # ----- Table / column behaviour -----
        th.Property(
            "table_name_pattern",
            th.StringType,
            default="${TABLE_NAME}",
            description="MySQL table name pattern",
        ),
        th.Property(
            "lower_case_table_names",
            th.BooleanType,
            default=True,
            description="Lower case table names",
        ),
        th.Property(
            "allow_column_alter",
            th.BooleanType,
            default=False,
            description="Allow column alter",
        ),
        th.Property(
            "replace_null",
            th.BooleanType,
            default=False,
            description="Replace null with type-appropriate blank value",
        ),
        th.Property(
            "default_string_length",
            th.IntegerType,
            default=255,
            description=(
                "Default VARCHAR length used when a Singer schema property of "
                "type 'string' has no `maxLength`. Was hard-coded to 1000 in "
                "0.1.x, which exceeds InnoDB's 3072-byte index limit on default "
                "MySQL 8 (utf8mb4 = 4 bytes/char): VARCHAR(1000) primary keys "
                "fail with `(1071, 'Specified key was too long')`. The 0.2 "
                "default of 255 fits comfortably; raise it for workloads where "
                "you control the charset and need wider strings."
            ),
        ),

        # ----- TLS / SSL -----
        th.Property(
            "ssl_mode",
            th.StringType,
            description=(
                "SSL mode. One of 'disabled', 'preferred', 'required', "
                "'verify_ca', 'verify_identity'. When unset, SSL is not "
                "configured (PyMySQL default; usually equivalent to "
                "'preferred')."
            ),
            allowed_values=[
                "disabled",
                "preferred",
                "required",
                "verify_ca",
                "verify_identity",
            ],
        ),
        th.Property(
            "ssl_ca",
            th.StringType,
            secret=True,
            description=(
                "Server CA certificate. Accepts either a filesystem path to a "
                "PEM file or the PEM content inline (the connector materializes "
                "inline content to a tempfile for the duration of the run)."
            ),
        ),
        th.Property(
            "ssl_cert",
            th.StringType,
            secret=True,
            description="Client certificate (path or inline PEM content).",
        ),
        th.Property(
            "ssl_key",
            th.StringType,
            secret=True,
            description="Client private key (path or inline PEM content).",
        ),
        th.Property(
            "ssl_cipher",
            th.StringType,
            description="Optional cipher suite list (passed to PyMySQL).",
        ),

        # ----- SSH tunnel -----
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    default=False,
                    description="Enable SSH tunnel via a bastion host.",
                ),
                th.Property(
                    "host",
                    th.StringType,
                    description="SSH bastion hostname.",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    default=22,
                    description="SSH bastion port.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="SSH username.",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    secret=True,
                    description="SSH private key (path or inline PEM content).",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    secret=True,
                    description="Optional private key passphrase.",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    secret=True,
                    description=(
                        "SSH user password (plain-password auth). Used when "
                        "the bastion does not accept key-based auth. "
                        "Mutually exclusive with private_key in practice — "
                        "if both are set, paramiko picks the key first."
                    ),
                ),
            ),
            description=(
                "Open an SSH tunnel to a bastion host before connecting to "
                "MySQL. Mirrors the config shape used by MeltanoLabs/tap-mysql."
            ),
        ),
    ).to_dict()

    schema_properties = {}

    def _process_lines(self, file_input: t.IO[str]) -> t.Counter[str]:
        if self.config.get("replace_null", False):
            processed_input = io.StringIO()
            for line in file_input:
                data = self.deserialize_json(line.strip())

                if data.get('type', '') == 'SCHEMA':
                    self.schema_properties = data['schema']['properties']
                elif data.get('type', '') == 'RECORD':
                    for key, value in data.get('record', {}).items():
                        if value is not None:
                            continue

                        # https://json-schema.org/understanding-json-schema/reference/type.html
                        _type = self.schema_properties[key]['type']
                        data_types = _type if isinstance(_type, list) else [_type]

                        if "null" in data_types:
                            continue
                        if "string" in data_types:
                            data['record'][key] = ""
                        elif "object" in data_types:
                            data['record'][key] = {}
                        elif "array" in data_types:
                            data['record'][key] = []
                        elif "boolean" in data_types:
                            data['record'][key] = False
                        else:
                            data['record'][key] = 0

                processed_input.write(json.dumps(data) + '\n')
            processed_input.seek(0)
            return super()._process_lines(processed_input)
        else:
            return super()._process_lines(file_input)


if __name__ == "__main__":
    TargetMySQL.cli()
