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

2. **JDBC fallback (packed driver)**  
   If the native driver is not available (e.g. on Windows without DB2 client), the library uses JDBC:
   - **Java (JRE)** must be installed.
   - **DB2 JDBC JAR** is taken from:
     - Package `drivers` folder: `datavalidation/drivers/` (e.g. place `db2jcc4.jar` there when building or shipping your app), or
     - Auto-download on first use to that folder or to `~/.datavalidation/drivers/`.

So you can “pack” DB2 support like in the Azure Migration Tool: put `db2jcc4.jar` in the `drivers` folder next to the `datavalidation` package (or rely on auto-download). No need to install the native IBM DB2 client when using the JDBC path.

## Packing the driver for distribution

- **Option A:** Add `db2jcc4.jar` (or equivalent) into `datavalidation/drivers/` before building/packaging your app. The library will find it there.
- **Option B:** Rely on auto-download on first run (requires network and write access to the drivers dir or `~/.datavalidation/drivers`).
- **Option C:** Set `DB2_JDBC_DRIVER_PATH` to the directory that contains the JAR.

JAR can be obtained from [Maven Central (IBM DB2 JCC)](https://mvnrepository.com/artifact/com.ibm.db2/jcc) or your IBM product install.
