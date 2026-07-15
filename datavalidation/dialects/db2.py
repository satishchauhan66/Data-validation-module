"""
DB2-specific SQL dialect (SYSCAT, SYSIBM).
"""
from datavalidation.dialects.base import SQLDialect


class DB2Dialect(SQLDialect):
    @property
    def name(self) -> str:
        return "db2"

    def _schema_filter(self, schema: str | None) -> str:
        """Filter for SYSCAT (TABSCHEMA)."""
        if not schema:
            return "1=1"
        s = str(schema).strip()
        return f"UPPER(RTRIM(TABSCHEMA)) = UPPER('{s}')"

    def _creator_filter(self, schema: str | None) -> str:
        """Filter for SYSIBM.SYSTABLES (CREATOR). Matches legacy backend."""
        if not schema:
            return "1=1"
        s = str(schema).strip()
        return f"UPPER(RTRIM(CREATOR)) = UPPER('{s}')"

    def catalog_tables_query(self, schema: str | None, object_types: list[str]) -> str:
        # Use SYSIBM.SYSTABLES (CREATOR/NAME) like legacy for reliable table list across DB2 setups
        type_cond = "1=1"
        if object_types:
            mapping = {"TABLE": "T", "VIEW": "V"}
            codes = [mapping.get(t.upper(), t) for t in object_types if t.upper() in ("TABLE", "VIEW")]
            if codes:
                type_cond = f"TYPE IN ({','.join(repr(c) for c in codes)})"
        return f"""
        SELECT RTRIM(CREATOR) AS schema_name, RTRIM(NAME) AS table_name, TYPE AS object_type
        FROM SYSIBM.SYSTABLES
        WHERE {self._creator_filter(schema)} AND {type_cond}
        ORDER BY CREATOR, NAME
        """

    def catalog_objects_query(self, schema: str | None, object_types: list[str]) -> str | None:
        """Unified query for TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER. Returns schema_name, table_name, object_type."""
        want = {t.upper() for t in (object_types or [])}
        if not want:
            want = {"TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER"}
        schema_cond = self._schema_filter(schema)
        parts = []
        if "TABLE" in want or "VIEW" in want:
            type_codes = []
            if "TABLE" in want:
                type_codes.append("'T'")
            if "VIEW" in want:
                type_codes.append("'V'")
            if type_codes:
                # SYSIBM.SYSTABLES (CREATOR/NAME) matches legacy backend
                creator_cond = self._creator_filter(schema)
                parts.append(f"""
                SELECT RTRIM(CREATOR) AS schema_name, RTRIM(NAME) AS table_name,
                       CASE TYPE WHEN 'T' THEN 'TABLE' WHEN 'V' THEN 'VIEW' END AS object_type
                FROM SYSIBM.SYSTABLES
                WHERE {creator_cond} AND TYPE IN ({','.join(type_codes)})
                """)
        proc_cond = f"PROCSCHEMA = '{schema.upper()}'" if schema else "1=1"
        func_cond = f"FUNCSCHEMA = '{schema.upper()}'" if schema else "1=1"
        trig_cond = f"TRIGSCHEMA = '{schema.upper()}'" if schema else "1=1"
        if "PROCEDURE" in want:
            parts.append(f"""
                SELECT PROCSCHEMA AS schema_name, PROCNAME AS table_name, 'PROCEDURE' AS object_type
                FROM SYSCAT.PROCEDURES WHERE {proc_cond}
                """)
        if "FUNCTION" in want:
            parts.append(f"""
                SELECT FUNCSCHEMA AS schema_name, FUNCNAME AS table_name, 'FUNCTION' AS object_type
                FROM SYSCAT.FUNCTIONS WHERE {func_cond}
                """)
        if "TRIGGER" in want:
            parts.append(f"""
                SELECT TRIGSCHEMA AS schema_name, TRIGNAME AS table_name, 'TRIGGER' AS object_type
                FROM SYSCAT.TRIGGERS WHERE {trig_cond}
                """)
        if not parts:
            return None
        return " UNION ALL ".join(f"({p.strip()})" for p in parts) + " ORDER BY schema_name, table_name"

    def catalog_presence_sequences_query(self, schema: str | None) -> str | None:
        """SEQUENCE presence: schema_name, object_name, object_type, seq_type, parent_table, parent_column.

        Returns standalone (``SEQTYPE = 'S'``) and identity (``SEQTYPE = 'I'``) sequences.
        ``seq_type`` / ``parent_table`` / ``parent_column`` let the Python layer map identity
        sequences to Azure ``IDENTITY`` columns (via SYSCAT.COLIDENTATTRIBUTES) and treat
        remapped ones as INFO instead of PRESENCE_MISSING_IN_TARGET.
        Alias sequences (``SEQTYPE = 'A'``) are excluded.
        """
        if schema:
            s_esc = str(schema).strip().replace("'", "''")
            schema_cond = f"UPPER(RTRIM(s.SEQSCHEMA)) = UPPER('{s_esc}')"
        else:
            schema_cond = "1=1"
        return f"""
        SELECT RTRIM(s.SEQSCHEMA) AS schema_name, RTRIM(s.SEQNAME) AS object_name, 'SEQUENCE' AS object_type,
               RTRIM(CAST(s.SEQTYPE AS CHAR(1))) AS seq_type,
               RTRIM(c.TABNAME) AS parent_table,
               RTRIM(c.COLNAME) AS parent_column
        FROM SYSCAT.SEQUENCES s
        LEFT JOIN SYSCAT.COLIDENTATTRIBUTES c ON c.SEQID = s.SEQID
        WHERE {schema_cond}
          AND s.SEQTYPE IN ('S', 'I')
        ORDER BY schema_name, object_name
        """

    def catalog_presence_indexes_query(self, schema: str | None) -> str | None:
        """INDEX presence by **table** schema (TABSCHEMA), not index schema (INDSCHEMA).

        Migration often owns the index in a different schema (e.g. CARVAR) while the table
        lives in USERID; Azure then creates the index under dbo. Filtering by INDSCHEMA
        would miss those and false-flag PRESENCE_MISSING_IN_SOURCE.
        Returns index_schema so the validator can emit INFO for cross-schema remaps.
        """
        if schema:
            s_esc = str(schema).strip().replace("'", "''")
            schema_cond = f"UPPER(RTRIM(TABSCHEMA)) = UPPER('{s_esc}')"
        else:
            schema_cond = "1=1"
        return f"""
        SELECT RTRIM(TABSCHEMA) AS schema_name,
               RTRIM(TABNAME) || '.' || RTRIM(INDNAME) AS object_name,
               'INDEX' AS object_type,
               RTRIM(INDSCHEMA) AS index_schema
        FROM SYSCAT.INDEXES
        WHERE {schema_cond} AND UNIQUERULE NOT IN ('P', 'U')
          AND UPPER(RTRIM(TABNAME)) NOT LIKE 'EXPLAIN\\_%' ESCAPE '\\'
        ORDER BY schema_name, object_name
        """

    def catalog_index_lookup_by_name_query(self, index_name: str, table_name: str | None = None) -> str:
        """Find an index by name across all schemas (optional table filter)."""
        ix = str(index_name).strip().replace("'", "''")
        tbl_cond = "1=1"
        if table_name:
            t = str(table_name).strip().replace("'", "''")
            tbl_cond = f"UPPER(RTRIM(TABNAME)) = UPPER('{t}')"
        return f"""
        SELECT RTRIM(INDSCHEMA) AS index_schema,
               RTRIM(TABSCHEMA) AS table_schema,
               RTRIM(TABNAME) AS table_name,
               RTRIM(INDNAME) AS index_name,
               RTRIM(UNIQUERULE) AS unique_rule
        FROM SYSCAT.INDEXES
        WHERE UPPER(RTRIM(INDNAME)) = UPPER('{ix}')
          AND {tbl_cond}
        ORDER BY index_schema, table_schema, table_name
        """

    def catalog_presence_constraints_query(self, schema: str | None) -> str | None:
        """CONSTRAINT presence: object_name = TABNAME.CONSTNAME; exclude FK and PK (F/P)."""
        schema_cond = f"UPPER(TABSCHEMA) = UPPER('{schema}')" if schema else "1=1"
        return f"""
        SELECT RTRIM(TABSCHEMA) AS schema_name,
               RTRIM(TABNAME) || '.' || RTRIM(CONSTNAME) AS object_name,
               'CONSTRAINT' AS object_type
        FROM SYSCAT.TABCONST
        WHERE {schema_cond} AND TYPE NOT IN ('F', 'P')
        ORDER BY schema_name, object_name
        """

    def catalog_columns_query(self, schema: str | None, table_name: str | None) -> str:
        schema_cond = self._schema_filter(schema).replace("TABSCHEMA", "C.TABSCHEMA")
        table_cond = "1=1" if not table_name else f"C.TABNAME = '{table_name.upper()}'"
        return f"""
        SELECT RTRIM(C.TABSCHEMA) AS schema_name, RTRIM(C.TABNAME) AS table_name, RTRIM(C.COLNAME) AS column_name,
               RTRIM(C.TYPENAME) AS data_type, C.LENGTH AS length, C.SCALE AS scale, RTRIM(C.NULLS) AS is_nullable,
               RTRIM(CAST(C.DEFAULT AS VARCHAR(32000))) AS column_default,
               CASE WHEN C.DEFAULT IS NOT NULL THEN 'COLUMN_DEFAULT' ELSE '' END AS default_constraint_name
        FROM SYSCAT.COLUMNS C
        WHERE {schema_cond} AND {table_cond}
        ORDER BY C.TABSCHEMA, C.TABNAME, C.COLNO
        """

    def catalog_columns_query_by_creator(self, schema: str | None) -> str:
        """Fallback column listing using SYSIBM.SYSCOLUMNS (TBCREATOR) when SYSCAT.COLUMNS (TABSCHEMA) returns nothing."""
        creator_cond = self._creator_filter(schema).replace("CREATOR", "C.TBCREATOR")
        return f"""
        SELECT RTRIM(C.TBCREATOR) AS schema_name, RTRIM(C.TBNAME) AS table_name, RTRIM(C.NAME) AS column_name,
               RTRIM(C.COLTYPE) AS data_type, C.LENGTH, C.SCALE, C.NULLS AS is_nullable,
               RTRIM(CAST(C.DEFAULT AS VARCHAR(32000))) AS column_default,
               CASE WHEN C.DEFAULT IS NOT NULL THEN 'COLUMN_DEFAULT' ELSE '' END AS default_constraint_name
        FROM SYSIBM.SYSCOLUMNS C
        WHERE {creator_cond}
        ORDER BY C.TBCREATOR, C.TBNAME, C.COLNO
        """

    def row_count_query(self, schema: str, table_name: str, dirty_read: bool = False) -> str:
        """Exact COUNT(*) for one DB2 table.

        With ``dirty_read=True`` appends ``WITH UR`` (Uncommitted Read isolation) so the count
        does not block on row locks held by writers — essential for multi-TB tables on busy systems.
        """
        s, t = schema.upper(), table_name.upper()
        sql = f'SELECT COUNT_BIG(*) AS cnt FROM "{s}"."{t}"'
        if dirty_read:
            sql += " WITH UR"
        return sql

    def row_count_estimate_query(self, schema: str, table_name: str) -> str | None:
        """Estimate row count from catalog statistics (no table scan).

        ``SYSCAT.TABLES.CARD`` is updated by ``RUNSTATS``. Returns -1 if statistics have never been
        collected, in which case the validator falls back per the configured mode.
        """
        s = schema.upper().replace("'", "''")
        t = table_name.upper().replace("'", "''")
        return f"""
        SELECT COALESCE(NULLIF(CARD, -1), 0) AS cnt
        FROM SYSCAT.TABLES
        WHERE UPPER(RTRIM(TABSCHEMA)) = UPPER('{s}')
          AND UPPER(RTRIM(TABNAME))   = UPPER('{t}')
        """

    def table_stats_query(self, schema: str | None) -> str | None:
        """Per-table CARD (rows) and FPAGES * page size (bytes) for sizing decisions."""
        if not schema:
            return None
        s = str(schema).strip().replace("'", "''")
        return f"""
        SELECT RTRIM(T.TABSCHEMA)                                AS schema_name,
               RTRIM(T.TABNAME)                                  AS table_name,
               CAST(T.CARD AS BIGINT)                            AS row_estimate,
               CAST(COALESCE(T.FPAGES, 0) * COALESCE(TS.PAGESIZE, 4096) AS BIGINT) AS bytes_estimate
        FROM SYSCAT.TABLES T
        LEFT JOIN SYSCAT.TABLESPACES TS ON T.TBSPACEID = TS.TBSPACEID
        WHERE UPPER(RTRIM(T.TABSCHEMA)) = UPPER('{s}')
          AND T.TYPE IN ('T','U')
        """

    def catalog_indexes_query(self, schema: str | None) -> str:
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT INDSCHEMA AS schema_name, INDNAME AS index_name, TABSCHEMA AS table_schema,
               TABNAME AS table_name, UNIQUERULE AS is_unique
        FROM SYSCAT.INDEXES
        WHERE {schema_cond}
        ORDER BY INDSCHEMA, INDNAME
        """

    def catalog_index_columns_query(self, schema: str | None) -> str | None:
        """Per original ``_fetch_db2_index_cols`` in pyspark_schema_comparison.py."""
        if not schema:
            return None
        s = str(schema).strip().replace("'", "''")
        return f"""
        SELECT RTRIM(i.tabschema) AS schema_name, RTRIM(i.tabname) AS table_name, RTRIM(i.indname) AS idx_name,
               RTRIM(i.uniquerule) AS unique_rule,
               ic.colseq AS colseq, RTRIM(ic.colname) AS col_name, RTRIM(ic.colorder) AS colorder
        FROM SYSCAT.INDEXES i
        JOIN SYSCAT.INDEXCOLUSE ic ON i.indschema = ic.indschema AND i.indname = ic.indname
        WHERE UPPER(RTRIM(i.tabschema)) = UPPER('{s}')
        ORDER BY i.tabschema, i.tabname, i.indname, ic.colseq
        """

    def catalog_fk_query(self, schema: str | None) -> str:
        schema_cond = self._schema_filter(schema) if schema else "1=1"
        return f"""
        SELECT RTRIM(CONSTNAME) AS fk_name, RTRIM(TABSCHEMA) AS schema_name, RTRIM(TABNAME) AS table_name,
               RTRIM(REFTABSCHEMA) AS ref_schema, RTRIM(REFTABNAME) AS ref_table,
               RTRIM(DELETERULE) AS delete_action, RTRIM(UPDATERULE) AS update_action
        FROM SYSCAT.REFERENCES
        WHERE {schema_cond}
        """

    def catalog_fk_columns_query(self, schema: str | None) -> str | None:
        if not schema:
            return None
        s = str(schema).strip().replace("'", "''")
        return f"""
        SELECT RTRIM(fk.CONSTNAME) AS fk_name,
               RTRIM(ref.TABSCHEMA) AS schema_name,
               RTRIM(ref.TABNAME) AS table_name,
               fk.COLSEQ AS col_seq,
               RTRIM(fk.COLNAME) AS fk_column,
               RTRIM(pk.COLNAME) AS pk_column
        FROM SYSCAT.REFERENCES ref
        JOIN SYSCAT.KEYCOLUSE fk
          ON fk.TABSCHEMA = ref.TABSCHEMA
         AND fk.TABNAME = ref.TABNAME
         AND fk.CONSTNAME = ref.CONSTNAME
        LEFT JOIN SYSCAT.KEYCOLUSE pk
          ON pk.TABSCHEMA = ref.REFTABSCHEMA
         AND pk.TABNAME = ref.REFTABNAME
         AND pk.CONSTNAME = ref.REFKEYNAME
         AND pk.COLSEQ = fk.COLSEQ
        WHERE UPPER(RTRIM(ref.TABSCHEMA)) = UPPER('{s}')
        ORDER BY ref.TABSCHEMA, ref.TABNAME, ref.CONSTNAME, fk.COLSEQ
        """

    def catalog_check_constraints_query(self, schema: str | None) -> str:
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT CONSTNAME AS constraint_name, TABSCHEMA AS schema_name, TABNAME AS table_name,
               TEXT AS check_clause
        FROM SYSCAT.CHECKS
        WHERE {schema_cond}
        """

    def checksum_query(self, schema: str, table_name: str, columns: list[str]) -> str:
        # DB2: use CHECKSUM_AGG or list columns; simple row hash
        col_list = ", ".join(f'"{c}"' for c in columns[:20])  # limit columns
        s, t = schema.upper(), table_name.upper()
        return f'SELECT CHECKSUM_AGG(CHECKSUM({col_list})) AS cs FROM "{s}"."{t}"'

    def checksum_row_fingerprint_query(
        self,
        schema: str,
        table_name: str,
        key_columns: list[str],
        value_columns: list[str],
    ) -> str | None:
        """Per-row key string + ``HASH`` fingerprint (hex) for value columns (Db2 LUW-style)."""
        if not key_columns:
            return None

        def qc(c: str) -> str:
            return '"' + str(c).upper().replace('"', '""') + '"'

        s = str(schema).upper().replace('"', '""')
        tn = str(table_name).upper().replace('"', '""')
        tbl = f'"{s}"."{tn}"'
        kcols = key_columns[:16]
        vcols = value_columns[:32]

        key_parts = [f"RTRIM(CAST({qc(k)} AS VARCHAR(32000)))" for k in kcols]
        if len(key_parts) == 1:
            key_expr = key_parts[0]
        else:
            key_expr = " CONCAT '|' CONCAT ".join(key_parts)

        if vcols:
            val_inner = " CONCAT ".join(f"COALESCE(CAST({qc(v)} AS VARCHAR(32000)), '')" for v in vcols)
        else:
            val_inner = "''"

        return f"""
SELECT CAST({key_expr} AS VARCHAR(32000)) AS KeySig,
       LOWER(HEX(HASH({val_inner}))) AS RowHash
FROM {tbl}
""".strip()
