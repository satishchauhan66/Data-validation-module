# Schema: Datatype Mapping

## Purpose

For each **column** in matched tables, compare:
1. **Type name**: Source data type vs target data type, and optionally vs an **expected** target type from a mapping (e.g. DB2 DECIMAL → Azure DECIMAL, DB2 VARCHAR → Azure NVARCHAR).
2. **Size**: Length (char types), precision/scale (numeric), datetime scale where applicable.

Report columns where the **name** or **size** of the type differs from the expected mapping.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.
- **Mapping rules** (config): Source type → expected Azure type (and optional size rules). Example: DECFLOAT → FLOAT, VARCHAR(n) → NVARCHAR(n), etc.

## Comparison logic

1. **Table/column pairs**: For each matched table, pair columns by normalized name.
2. **Source metadata**: For each column, get source data type name and size (length, precision, scale, datetime scale).
3. **Target metadata**: Same for target.
4. **Expected type**: Using mapping rules, compute expected target type (and size) from source type.
5. **Compare**:
   - **Type name mismatch**: Actual target type name != expected type name (e.g. expected DECFLOAT, actual FLOAT).
   - **Size mismatch**: Length, precision, scale, or datetime scale differs (where applicable).
6. **Status**: Some mappings can be treated as **warning** (e.g. VARCHAR→VARBINARY by rule) vs **error**.

## Error codes

| Code | Meaning |
|------|--------|
| DATATYPE_NAME_MISMATCH | Target type name does not match expected (from mapping). |
| DATATYPE_SIZE_MISMATCH | Length, precision, scale, or datetime scale differs. |
| DATATYPE_MISMATCH | Generic type mismatch. |

## Output (unified row)

- **ValidationType**: `datatype_mapping`
- **Status**: `error` or `warning` (e.g. by rule).
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable.ColumnName`
- **ErrorDescription**: e.g. "Data type name mismatch", "Data type size mismatch".
- **DetailsJson**: JSON object with:
  - `column_name`: string  
  - `source_data_type`: string  
  - `destination_data_type`: string  
  - `expected_azure_type`: string (from mapping)  
  - `actual_azure_type`: string  
  - `source_precision`: number | null  
  - `source_scale`: number | null  
  - `destination_precision`: number | null  
  - `destination_scale`: number | null  

(Include character length fields if your model has them.)

## DB-specific notes

- **DB2**: Type names and attributes from SYSCAT.COLUMNS / SYSCAT.DATATYPES; length, scale, etc.
- **Azure SQL**: `sys.types`, `sys.columns` (max_length, precision, scale); type names may differ (e.g. nvarchar vs NVARCHAR). Normalize for comparison.
