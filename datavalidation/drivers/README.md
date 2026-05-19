# DB2 JDBC driver (bundled)

`db2jcc4.jar` is shipped with the `datavalidation` package and installed via pip into this folder.

- **Windows / PyInstaller apps:** the library uses JDBC by default (no IBM CLI install).
- **Java (JRE)** must be on the machine for JDBC; the library discovers `JAVA_HOME` / Program Files automatically.
- **Refresh jar:** download [jcc 11.5.9.0](https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar) and save as `db2jcc4.jar` here before building a release.
