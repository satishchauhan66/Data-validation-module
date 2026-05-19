# DB2 support setup

The library supports DB2 as source or target. A single install includes everything:

```bash
pip install datavalidation
```

This installs:

- `ibm_db`, `ibm_db_sa` – native DB2 driver (requires IBM DB2 client on the machine)
- `jaydebeapi`, `jpype1` – JDBC bridge (uses Java + DB2 JAR when native driver is unavailable)
- `packaging` – required by `ibm_db_sa`

## 2. How the library connects to DB2

1. **Native (ibm_db)**  
   Tries first. Needs [IBM Data Server Driver / DB2 client](https://www.ibm.com/support/pages/getting-started-ibm-data-server-drivers) installed on the machine. On Windows this often means installing the full client.

2. **JDBC (bundled driver, default on Windows)**  
   `db2jcc4.jar` is included in the pip package under `datavalidation/drivers/`. No user configuration is required.
   - **Java (JRE)** must be installed on the machine (64-bit if your app is 64-bit).
   - On Windows and PyInstaller builds, the library skips native `ibm_db` and uses JDBC automatically.

Auto-download to `~/.datavalidation/drivers` only runs if the bundled jar is missing (e.g. incomplete source checkout).

## PyInstaller / DB2 Migration Tool

Include package data in your spec so the jar is extracted next to the app:

- `datavalidation/drivers/db2jcc4.jar`
- `jaydebeapi`, `jpype1`

The library resolves the jar via `importlib.resources`, the package `drivers/` folder, and `sys._MEIPASS` when frozen.

## Windows: `DLL load failed while importing ibm_db`

The Python package `ibm_db` is only a wrapper; it needs IBM’s native **Data Server Driver / CLIDriver** DLLs on the machine (`db2cli.dll`, etc.). Packaged apps (e.g. PyInstaller) often ship `ibm_db` without those DLLs, which produces:

`ImportError: DLL load failed while importing ibm_db: The specified module could not be found.`

**Recommended:** `pip install datavalidation` (includes `db2jcc4.jar`) and install **Java JRE**. On Windows, JDBC is used by default.

**Alternative:** install [IBM Data Server Driver Package](https://www.ibm.com/support/pages/download-initial-version-115-clidriver-and-odbc-driver) and set `DV_DB2_USE_JDBC=0` to force native `ibm_db` on Windows.
