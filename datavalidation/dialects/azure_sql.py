"""
Azure SQL / SQL Server-specific SQL dialect (sys.*, INFORMATION_SCHEMA).
"""
from datavalidation.dialects.base import SQLDialect


class AzureSQLDialect(SQLDialect):
    @property
    def name(self) -> str:
        return "azure_sql"

    def _schema_filter(self, schema: str | None) -> str:
        if not schema:
            return "1=1"
        return f"s.name = N'{schema.replace(chr(39), chr(39)+chr(39))}'"

    def _schema_filter_by_schema_id(self, schema: str | None, schema_id_column: str = "schema_id") -> str:
        """WHERE fragment when the query has no ``sys.schemas`` alias ``s`` (e.g. ``sys.sequences``)."""
        if not schema:
            return "1=1"
        esc = str(schema).strip().replace("'", "''")
        return f"SCHEMA_NAME({schema_id_column}) = N'{esc}'"

    def catalog_tables_query(self, schema: str | None, object_types: list[str]) -> str:
        type_cond = "1=1"
        if object_types:
            mapping = {"TABLE": "U", "VIEW": "V"}
            codes = [mapping.get(t.upper(), "U") for t in object_types if t.upper() in ("TABLE", "VIEW")]
            codes = list(dict.fromkeys(codes))
            if codes:
                type_cond = f"o.type IN ({','.join(repr(c) for c in codes)})"
        return f"""
        SELECT s.name AS schema_name, o.name AS table_name, o.type AS object_type
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE {self._schema_filter(schema)} AND {type_cond}
        AND o.is_ms_shipped = 0
        ORDER BY s.name, o.name
        """

    def catalog_objects_query(self, schema: str | None, object_types: list[str]) -> str | None:
        """Unified query for TABLE, VIEW, PROCEDURE, FUNCTION, TRIGGER. Returns schema_name, table_name, object_type."""
        want = {t.upper() for t in (object_types or [])}
        if not want:
            want = {"TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER"}
        type_map = {"TABLE": "U", "VIEW": "V", "PROCEDURE": "P", "TRIGGER": "TR", "FUNCTION": "FN"}
        types_sql = []
        for w in want:
            if w in type_map:
                types_sql.append(repr(type_map[w]))
            if w == "FUNCTION":
                types_sql.extend(["'IF'", "'TF'"])
        types_sql = list(dict.fromkeys(types_sql))
        if not types_sql:
            return None
        type_cond = f"o.type IN ({','.join(types_sql)})"
        return f"""
        SELECT s.name AS schema_name, o.name AS table_name,
               CASE o.type WHEN 'U' THEN 'TABLE' WHEN 'V' THEN 'VIEW' WHEN 'P' THEN 'PROCEDURE'
                           WHEN 'TR' THEN 'TRIGGER' WHEN 'FN' THEN 'FUNCTION' WHEN 'IF' THEN 'FUNCTION' WHEN 'TF' THEN 'FUNCTION' ELSE o.type END AS object_type
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE {self._schema_filter(schema)} AND {type_cond} AND o.is_ms_shipped = 0
        ORDER BY s.name, o.name
        """

    def catalog_presence_sequences_query(self, schema: str | None) -> str | None:
        """SEQUENCE presence: schema_name, object_name = name."""
        schema_cond = self._schema_filter_by_schema_id(schema, "schema_id")
        return f"""
        SELECT RTRIM(SCHEMA_NAME(schema_id)) AS schema_name, RTRIM(name) AS object_name, 'SEQUENCE' AS object_type
        FROM sys.sequences
        WHERE {schema_cond}
        ORDER BY schema_name, object_name
        """

    def catalog_presence_indexes_query(self, schema: str | None) -> str | None:
        """INDEX presence: object_name = TableName.IndexName; exclude hypothetical, PK, unique (match old backend)."""
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT RTRIM(s.name) AS schema_name,
               RTRIM(t.name) + '.' + RTRIM(i.name) AS object_name,
               'INDEX' AS object_type
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE {schema_cond} AND i.is_hypothetical = 0 AND i.name IS NOT NULL AND i.is_primary_key = 0 AND i.is_unique = 0
        ORDER BY schema_name, object_name
        """

    def catalog_presence_constraints_query(self, schema: str | None) -> str | None:
        """CONSTRAINT presence: key_constraints (non-PK) + check + default; object_name = TableName.ConstraintName."""
        schema_cond = self._schema_filter(schema)
        q1 = f"""
        SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) + '.' + RTRIM(kc.name) AS object_name, 'CONSTRAINT' AS object_type
        FROM sys.key_constraints kc
        INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE {schema_cond} AND kc.type <> 'PK'
        """
        q2 = f"""
        SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) + '.' + RTRIM(cc.name) AS object_name, 'CONSTRAINT' AS object_type
        FROM sys.check_constraints cc
        INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE {schema_cond}
        """
        q3 = f"""
        SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) + '.' + RTRIM(dc.name) AS object_name, 'CONSTRAINT' AS object_type
        FROM sys.default_constraints dc
        INNER JOIN sys.tables t ON dc.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE {schema_cond}
        """
        return f"({q1}) UNION ALL ({q2}) UNION ALL ({q3}) ORDER BY schema_name, object_name"

    def catalog_columns_query(self, schema: str | None, table_name: str | None) -> str:
        schema_cond = self._schema_filter(schema)
        table_cond = "1=1"
        if table_name:
            table_cond = f"t.name = N'{table_name.replace(chr(39), chr(39)+chr(39))}'"
        return f"""
        SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name,
               ty.name AS data_type, c.max_length AS length, c.scale AS scale,
               c.is_nullable AS is_nullable,
               ISNULL(dc.definition, N'') AS column_default
        FROM sys.columns c
        INNER JOIN sys.tables t ON c.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        LEFT JOIN sys.default_constraints dc
          ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
        WHERE {schema_cond} AND {table_cond}
        ORDER BY s.name, t.name, c.column_id
        """

    def row_count_query(self, schema: str, table_name: str, dirty_read: bool = False) -> str:
        """Exact ``COUNT_BIG(*)`` for one table, optionally with the ``WITH (NOLOCK)`` table hint
        so the count does not block on uncommitted writers (read-uncommitted)."""
        s = schema.replace("]", "]]")
        t = table_name.replace("]", "]]")
        hint = " WITH (NOLOCK)" if dirty_read else ""
        return f"SELECT COUNT_BIG(*) AS cnt FROM [{s}].[{t}]{hint}"

    def row_count_estimate_query(self, schema: str, table_name: str) -> str | None:
        """Estimate row count from ``sys.dm_db_partition_stats`` (heap or clustered index)."""
        s = schema.replace("'", "''")
        t = table_name.replace("'", "''")
        return f"""
        SELECT CAST(SUM(p.row_count) AS BIGINT) AS cnt
        FROM sys.dm_db_partition_stats p
        INNER JOIN sys.objects o ON o.object_id = p.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = N'{s}' AND o.name = N'{t}' AND p.index_id IN (0, 1)
        """

    def table_stats_query(self, schema: str | None) -> str | None:
        """Per-table row + bytes estimate from sys.dm_db_partition_stats (instant)."""
        if not schema:
            return None
        s = str(schema).strip().replace("'", "''")
        return f"""
        SELECT s.name AS schema_name,
               o.name AS table_name,
               CAST(SUM(CASE WHEN p.index_id IN (0,1) THEN p.row_count ELSE 0 END) AS BIGINT) AS row_estimate,
               CAST(SUM(p.in_row_used_page_count + p.lob_used_page_count + p.row_overflow_used_page_count) * 8 * 1024 AS BIGINT) AS bytes_estimate
        FROM sys.dm_db_partition_stats p
        INNER JOIN sys.objects o ON o.object_id = p.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = N'{s}' AND o.is_ms_shipped = 0 AND o.type = 'U'
        GROUP BY s.name, o.name
        """

    def catalog_indexes_query(self, schema: str | None) -> str:
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT s.name AS schema_name, i.name AS index_name, s.name AS table_schema,
               t.name AS table_name, i.is_unique AS is_unique
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE {schema_cond} AND i.type > 0
        ORDER BY s.name, i.name
        """

    def catalog_index_columns_query(self, schema: str | None) -> str | None:
        """Per original ``_fetch_sql_index_cols`` in pyspark_schema_comparison.py."""
        if not schema:
            return None
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT RTRIM(s.name) AS schema_name, RTRIM(t.name) AS table_name, RTRIM(i.name) AS idx_name,
               CAST(i.is_unique AS int) AS is_unique,
               CAST(i.is_primary_key AS int) AS is_primary_key,
               ic.key_ordinal AS colseq, RTRIM(c.name) AS col_name,
               CAST(ic.is_descending_key AS int) AS is_descending_key
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
        WHERE i.is_hypothetical = 0 AND i.name IS NOT NULL AND {schema_cond}
        ORDER BY s.name, t.name, i.name, ic.key_ordinal
        """

    def catalog_fk_query(self, schema: str | None) -> str:
        schema_cond = self._schema_filter(schema) if schema else "1=1"
        return f"""
        SELECT fk.name AS fk_name, s.name AS schema_name, t.name AS table_name,
               rs.name AS ref_schema, rt.name AS ref_table,
               fk.delete_referential_action AS delete_action,
               fk.update_referential_action AS update_action
        FROM sys.foreign_keys fk
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
        INNER JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
        WHERE {schema_cond}
        """

    def catalog_fk_columns_query(self, schema: str | None) -> str | None:
        if not schema:
            return None
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT fk.name AS fk_name, s.name AS schema_name, t.name AS table_name,
               fkc.constraint_column_id AS col_seq,
               cp.name AS fk_column, cr.name AS pk_column
        FROM sys.foreign_key_columns fkc
        INNER JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
        INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.columns cp ON cp.object_id = fk.parent_object_id AND cp.column_id = fkc.parent_column_id
        INNER JOIN sys.columns cr ON cr.object_id = fk.referenced_object_id AND cr.column_id = fkc.referenced_column_id
        WHERE {schema_cond}
        ORDER BY s.name, t.name, fk.name, fkc.constraint_column_id
        """

    def catalog_check_constraints_query(self, schema: str | None) -> str:
        schema_cond = self._schema_filter(schema)
        return f"""
        SELECT cc.name AS constraint_name, s.name AS schema_name, t.name AS table_name,
               cc.definition AS check_clause
        FROM sys.check_constraints cc
        INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE {schema_cond}
        """

    def checksum_query(self, schema: str, table_name: str, columns: list[str]) -> str:
        col_list = ", ".join(f"[{c}]" for c in columns[:32])
        s = schema.replace("]", "]]")
        t = table_name.replace("]", "]]")
        return f"SELECT CHECKSUM_AGG(CHECKSUM({col_list})) AS cs FROM [{s}].[{t}]"

    def checksum_row_fingerprint_query(
        self,
        schema: str,
        table_name: str,
        key_columns: list[str],
        value_columns: list[str],
    ) -> str | None:
        """Per-row ``KeySig`` + SHA-256 hex ``RowHash`` for non-key columns (legacy-style row checksum)."""
        if not key_columns:
            return None

        def br(c: str) -> str:
            return f"[{str(c).replace(']', ']]')}]"

        s = schema.replace("]", "]]")
        t = table_name.replace("]", "]]")
        kcols = key_columns[:16]
        vcols = value_columns[:32]

        if len(kcols) == 1:
            key_expr = f"CAST({br(kcols[0])} AS NVARCHAR(MAX))"
        else:
            key_expr = "CONCAT_WS(N'|', " + ", ".join(f"CAST({br(c)} AS NVARCHAR(MAX))" for c in kcols) + ")"

        if vcols:
            val_expr = "CONCAT_WS(N'||', " + ", ".join(
                f"ISNULL(CAST({br(c)} AS NVARCHAR(MAX)), N'<NULL>')" for c in vcols
            ) + ")"
        else:
            val_expr = "N''"

        return f"""
SELECT CAST({key_expr} AS NVARCHAR(MAX)) AS KeySig,
       LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {val_expr}), 2)) AS RowHash
FROM [{s}].[{t}]
""".strip()
