# Behavior: Identity / Sequence

## Purpose

Compare **identity column** and **sequence** definitions between source and target:
- **Identity**: seed, increment, current (last) value per table/column.
- **Sequence**: same concepts for standalone sequence objects.

Report mismatches (e.g. different seed, increment, or current value).

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: TABLE for identity columns; SEQUENCE for sequences.

## Comparison logic

1. **Identity columns**: For each table present in both DBs, list columns that are identity (auto-increment). Get seed_value, increment_value, last_value (or equivalent) from source and target catalogs.
2. **Sequences**: List standalone sequence objects; get seed/increment/current value from both catalogs.
3. **Match**: Match by table/column for identity; by schema/sequence name for sequences.
4. **Compare**: Emit a row when seed, increment, or current value differs (or when one side has identity/sequence and the other does not).

## Error codes

(Implementation-specific; e.g. identity/sequence mismatch by type. Use a single **ErrorCode** like `IDENTITY_MISMATCH` or `SEQUENCE_MISMATCH` with **ErrorDescription** detailing seed/increment/current.)

## Output (unified row)

- **ValidationType**: `identity_sequence` or `IdentitySequence` / `Sequence`
- **Status**: `error` (or `warning`)
- **ObjectType**: TABLE (identity) or SEQUENCE
- **ElementPath**: e.g. `Schema.Table.Column` or `Schema.SequenceName`
- **DetailsJson**: JSON with e.g. seed_value, increment_value, last_value (or current value), and source vs destination.

## DB-specific notes

- **DB2**: Identity from SYSCAT.COLUMNS or catalog identity metadata; sequences from SYSCAT.SEQUENCES (seed, increment, current value).
- **Azure SQL**: Identity from `sys.identity_columns` (seed_value, increment_value, last_value); sequences from `sys.sequences`.
