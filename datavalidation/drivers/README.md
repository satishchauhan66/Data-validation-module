# DB2 JDBC driver (optional)

For DB2 connections, the library can use:

1. **Native driver** (`ibm_db` + IBM DB2 client) – requires IBM Data Server Driver/Client installed on the machine.
2. **JDBC driver** – requires Java (JRE) and `db2jcc4.jar` in this folder (or auto-downloaded on first use).

If you use **JDBC** (e.g. when the native driver is not available on Windows), place `db2jcc4.jar` here, or let the library download it from Maven Central on first run.

- **Manual:** Download [IBM DB2 JCC driver](https://mvnrepository.com/artifact/com.ibm.db2/jcc) and put `db2jcc4.jar` (or `jcc-*.jar` renamed) in this `drivers` folder.
- **Auto:** The library will try to download the driver to this folder or to `~/.datavalidation/drivers` if this folder is not writable.
