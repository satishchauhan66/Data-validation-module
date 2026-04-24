# Data: Checksum / Hash

## Purpose

For each matched table, use a **key** (e.g. primary key) to:
1. Compute a **per-row hash** (e.g. hash of concatenated column values or of a canonical row representation) for each key in source and target.
2. Compare key-by-key: keys only in source → MISSING_IN_TARGET; only in target → MISSING_IN_SOURCE; in both but different hash → ROW_HASH_MISMATCH.
3. **Optional**: If no key exists, fall back to an **unordered set hash** (e.g. hash of sorted row hashes) and compare single aggregate; report UNORDERED_HASH_MISMATCH or NO_KEY_NO_UNORDERED if fallback fails.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically TABLE.
- **Key**: Primary key or configured key columns.
- **Hash algorithm**: e.g. MD5, SHA-256 of a canonical string (column values concatenated with delimiter, or JSON).

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Resolve key**: Primary key or configured key.
3. **Source**: For each row, compute KeySig (e.g. concat key values) and RowHash (hash of non-key columns or full row). Stream or batch to avoid OOM.
4. **Target**: Same.
5. **Compare**:
   - Keys in source only → MISSING_IN_TARGET (key present only in source).
   - Keys in target only → MISSING_IN_SOURCE (key present only in target).
   - Key in both but RowHash differs → ROW_HASH_MISMATCH.
6. **No key**: If no key, compute unordered set hash (e.g. SORT all row hashes, concatenate, hash) for source and target; if different → UNORDERED_HASH_MISMATCH. If comparison fails → NO_KEY_NO_UNORDERED.
7. **Read/hash failure**: READ_FAILED with error description.

## Error codes

| Code | Meaning |
|------|--------|
| READ_FAILED | Read or hash computation failed. |
| UNORDERED_HASH_MISMATCH | Unordered row set hash differs (no key comparison). |
| NO_KEY_NO_UNORDERED | No key and unordered comparison failed. |
| MISSING_IN_TARGET | Key present only in source. |
| MISSING_IN_SOURCE | Key present only in target. |
| ROW_HASH_MISMATCH | Same key, different row hash. |

## Output (unified row)

- **ValidationType**: `checksum_hash`
- **Status**: `error`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `Schema.Table`
- **DetailsJson**: JSON object with:
  - `key_columns`: array (or string list)  
  - `key` or `key_type`: key signature or type  
  - `row_hash`: when applicable  
  - `source_count`, `target_count` (or similar) when applicable  
  - `differing_columns_sample`: optional, for ROW_HASH_MISMATCH  

## DB-specific notes

- Hashing: use same encoding and delimiter in both DBs (e.g. cast to VARCHAR, concatenate, then hash). Handle NULL consistently (e.g. 'NULL' or ''). Azure: HASHBYTES('SHA2_256', ...); DB2: may use application-side hash or DB function if available.
