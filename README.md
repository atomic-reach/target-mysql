# target-mysql

`target-mysql` is a MySQL-focused Singer target, crafted with the [Meltano Target SDK](https://sdk.meltano.com).

This repository is the [atomic-reach](https://github.com/atomic-reach/target-mysql) fork of [thkwag/target-mysql](https://github.com/thkwag/target-mysql), adding native SSL and SSH-tunnel configuration, a pure-Python default driver, configurable VARCHAR length, and a friendlier error for InnoDB's 3072-byte key-length limit. See [What's new in 0.2.0](#whats-new-in-020) below.

English | [한국어](./docs/README_ko.md)


## Installation

Install the published thkwag release from PyPI (does **not** include the atomic-reach 0.2.x changes):

```bash
pip install thk-target-mysql
```

Install the atomic-reach fork directly from this repo:

```bash
pipx install git+https://github.com/atomic-reach/target-mysql.git@v0.2.0
```

Or pin to a specific commit / branch in your `meltano.yml`:

```yaml
plugins:
  loaders:
    - name: target-mysql
      variant: thkwag
      pip_url: git+https://github.com/atomic-reach/target-mysql.git@v0.2.0 'setuptools<80'
      # Python 3.11 — see Python compatibility note below.
      python: "3.11"
      config:
        sqlalchemy_url: ${TARGET_MYSQL_SQLALCHEMY_URL}
```

### Python compatibility

`pyproject.toml` declares `python = ">=3.8.1,<3.14"`, but at runtime the loader pulls in Singer SDK 0.30, which still pins `pendulum ^2.1`. `pendulum 2.1.2` has no pre-built wheel for Python 3.12+ and fails to build from source on those versions. **In practice the loader needs a Python 3.11 venv** until Singer SDK ships with `pendulum 3.x` support.

### `pkg_resources` / `setuptools < 80`

Singer SDK 0.30 (via `PyFilesystem`'s `fs` dependency) still imports `pkg_resources`, which was **removed from `setuptools >= 80`**. If your install environment ships modern setuptools, bundle an older one into the loader's venv:

```bash
pip install thk-target-mysql 'setuptools<80'
```

Without this, `target-mysql --help` and `meltano invoke target-mysql` crash with `ModuleNotFoundError: No module named 'pkg_resources'` before the plugin can even read its config.

## Configuration

The available configuration options for `target-mysql` are:

### Connection

| Option            | Description                                                                                                | Default            |
|-------------------|------------------------------------------------------------------------------------------------------------|--------------------|
| `sqlalchemy_url`  | Full SQLAlchemy URL. When set, overrides the discrete host/port/user/password/database fields.             |                    |
| `driver_name`     | SQLAlchemy driver name. `mysql+pymysql` is pure-Python and has no system build deps. Use `mysql` for the `mysqlclient` C extension (requires `pip install mysqlclient` and system libs). | `mysql+pymysql`    |
| `host`            | MySQL server's hostname or IP address                                                                       |                    |
| `port`            | Port where MySQL server is running                                                                          | `3306`             |
| `username`        | MySQL username                                                                                              |                    |
| `password`        | MySQL user's password                                                                                       |                    |
| `database`        | MySQL database's name                                                                                       |                    |

### Table / column behaviour

| Option                  | Description                                                                                                            | Default            |
|-------------------------|------------------------------------------------------------------------------------------------------------------------|--------------------|
| `table_name_pattern`    | MySQL table name pattern                                                                                                | `"${TABLE_NAME}"`  |
| `lower_case_table_names`| Use lowercase for table names                                                                                          | `true`             |
| `allow_column_alter`    | Allow column alterations                                                                                                | `false`            |
| `replace_null`          | Replace null values with type-appropriate blanks (experimental)                                                         | `false`            |
| `default_string_length` | VARCHAR length used when a Singer schema property of type `string` has no `maxLength`. **Was hard-coded to 1000 in 0.1.x.** See [VARCHAR length](#varchar-length-and-the-3072-byte-key-limit) below. | `255`              |

### TLS / SSL

PEM content fields accept either a **filesystem path** to a PEM file or the **inline PEM content** (recognised by the `-----BEGIN ` prefix). Inline content is materialised to a tempfile for the duration of the run and unlinked at process exit.

| Option       | Description                                                                                                |
|--------------|------------------------------------------------------------------------------------------------------------|
| `ssl_mode`   | One of `disabled`, `preferred`, `required`, `verify_ca`, `verify_identity`. Unset → PyMySQL default.       |
| `ssl_ca`     | Server CA certificate (path or inline PEM). Required for `verify_ca` / `verify_identity`.                  |
| `ssl_cert`   | Client certificate (path or inline PEM).                                                                   |
| `ssl_key`    | Client private key (path or inline PEM).                                                                   |
| `ssl_cipher` | Optional cipher suite list passed to PyMySQL.                                                              |

### SSH tunnel

Open an SSH tunnel to a bastion host before connecting to MySQL. Mirrors the config shape used by [MeltanoLabs/tap-mysql](https://github.com/MeltanoLabs/tap-mysql). The forwarder is held for the lifetime of the SQLAlchemy engine.

| Option                                | Description                                                                                                        | Default |
|---------------------------------------|--------------------------------------------------------------------------------------------------------------------|---------|
| `ssh_tunnel.enable`                   | Enable the SSH tunnel.                                                                                              | `false` |
| `ssh_tunnel.host`                     | SSH bastion hostname.                                                                                               |         |
| `ssh_tunnel.port`                     | SSH bastion port.                                                                                                   | `22`    |
| `ssh_tunnel.username`                 | SSH username.                                                                                                       |         |
| `ssh_tunnel.private_key`              | SSH private key (path or inline PEM content).                                                                       |         |
| `ssh_tunnel.private_key_password`     | Optional private key passphrase.                                                                                    |         |
| `ssh_tunnel.password`                 | SSH user password. Used when the bastion does not accept key auth. If both `private_key` and `password` are set, paramiko prefers the key. |         |

Configurations can be stored in a JSON configuration file and specified using the `--config` flag with `target-mysql`.

### VARCHAR length and the 3072-byte key limit

`default_string_length` defaults to **255** in 0.2.x (it was hard-coded to 1000 in 0.1.x). The previous default exceeded InnoDB's 3072-byte index limit on a default MySQL 8 install (utf8mb4 = 4 bytes/char × 1000 = 4000 bytes), causing primary-key creation to fail with `(1071, 'Specified key was too long; max key length is 3072 bytes')` for any auto-PK'd table whose key was an auto-generated `VARCHAR(1000)` column.

The 0.2 default of 255 fits comfortably (255 × 4 = 1020 bytes). Raise it for workloads where you control the charset (e.g. utf8mb3 or latin1) and need wider auto-PK strings. When the loader hits the 1071 error it now logs a guidance message pointing at this config key.

### The `replace_null` option (experimental)

By enabling `replace_null`, null values are replaced with 'empty' equivalents based on their data type. Use with caution as it may alter data semantics.

| JSON Schema Data Type | Null Value Replacement |
|-----------------------|------------------------|
| string                | Empty string (`""`)    |
| number                | `0`                    |
| object                | Empty object (`{}`)    |
| array                 | Empty array (`[]`)     |
| boolean               | `false`                |
| null                  | null                   |


## Usage

```bash
cat <input_stream> | target-mysql --config <config.json>
```

- `<input_stream>`: Input data stream
- `<config.json>`: JSON configuration file

`target-mysql` reads data from a Singer Tap and writes it to a MySQL database. Run a Singer Tap to generate data before launching `target-mysql`.

Example with the exchange-rates tap:

```bash
tap-exchangeratesapi | target-mysql --config config.json
```

`config.json` contains `target-mysql` settings.

## What's new in 0.2.0

The atomic-reach fork's 0.2.0 release adds:

- **PyMySQL as the default driver.** `driver_name` defaults to `mysql+pymysql` so no system libmysqlclient / pkg-config is required to install the loader. The `mysqlclient` C-extension remains supported and is faster — install it explicitly and set `driver_name=mysql` if you want it.
- **Native SSL config.** `ssl_mode`, `ssl_ca`, `ssl_cert`, `ssl_key`, `ssl_cipher` are first-class options. Inline PEM content is accepted (handy when certs come from a secret store like Vault). Previously SSL had to be tunnelled through `sqlalchemy_url` query params.
- **Native SSH tunnel.** `ssh_tunnel.enable` + host/port/username + key/password authentication. Mirrors `MeltanoLabs/tap-mysql`'s shape so source/destination configs can share the same secrets model. Previously the loader had no SSH support.
- **Configurable VARCHAR length** via `default_string_length` (default `255`, was hard-coded to `1000`). Fixes `(1071, 'Specified key was too long')` for auto-PK'd tables on default MySQL 8 / utf8mb4 installs.
- **Friendlier 1071 error message** pointing operators at the `default_string_length` knob (and at the charset they may want to switch to).
- **Wider Python support** in `pyproject.toml` (`>=3.8.1,<3.14`). At runtime you'll still need Python 3.11 until Singer SDK 0.30's `pendulum 2.x` pin is lifted — see [Python compatibility](#python-compatibility).
- **`sshtunnel ^0.4.0`** added as a runtime dependency.

The CLI / Singer protocol surface is unchanged — existing configs continue to work; all new fields are optional and default to the previous behaviour. `driver_name`'s default did change from `mysql` to `mysql+pymysql`; pin it back to `mysql` explicitly if you depend on the C extension.


## Developer resources

### Initializing the development environment

```bash
pipx install poetry
poetry install
```

### Creating and running tests

Create tests in the `target_mysql/tests` subfolder and run:

```bash
poetry run pytest
```

Use `poetry run` to test the `target-mysql` CLI:

```bash
poetry run target-mysql --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target functions within a Singer environment and does not require Meltano._

```bash
pipx install meltano
cd target-mysql
meltano install

# Smoke-test:
meltano invoke target-mysql --version

# Or run an end-to-end pipeline with the Carbon Intensity tap:
meltano run tap-carbon-intensity target-mysql
```

### SDK development guide

For in-depth instructions on crafting Singer Taps and Targets using the Meltano Singer SDK, see the [Development Guide](https://sdk.meltano.com/en/latest/dev_guide.html).

## Reference links

- [Meltano Target SDK Documentation](https://sdk.meltano.com)
- [Singer Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
- [Meltano](https://meltano.com/)
- [Singer.io](https://www.singer.io/)