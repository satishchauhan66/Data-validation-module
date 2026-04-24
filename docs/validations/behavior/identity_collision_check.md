# Behavior: Identity Collision Check

## Purpose

Assess **risk of identity/sequence value collision** after migration (e.g. when multiple tables or sequences will share a single target namespace, or when seed/increment could cause overlap). Run checks per identity column or sequence: e.g. compare current value vs max theoretical safe value, or flag tables with overlapping ranges. Report rows when collision risk is detected.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: TABLE (identity), SEQUENCE.

## Comparison logic

1. **Identity/sequence list**: From source (and optionally target), get identity columns and sequences with seed, increment, last_value (current value).
2. **Risk rules** (implementation-defined): e.g.  
   - If last_value is close to max for the data type (e.g. INT near 2^31-1), flag as risk.  
   - If multiple identities could overlap after merge, flag.  
   - If increment and current value suggest exhaustion before next maintenance, flag.  
3. **Report**: One row per identity/sequence (or per table) where risk is above threshold.

## Error codes

| Code | Meaning |
|------|--------|
| IDENTITY_COLLISION_RISK | Identity or sequence has elevated collision or exhaustion risk. |

## Output (unified row)

- **ValidationType**: `identity_collision_check`
- **Status**: `error` or `warning`
- **ObjectType**: TABLE or SEQUENCE
- **ElementPath**: e.g. `Schema.Table.Column` or `Schema.SequenceName`
- **DetailsJson**: JSON with e.g. seed_value, increment_value, last_value, risk reason (exhaustion, overlap, etc.).

## DB-specific notes

- **DB2**: Current value from identity/sequence catalog.
- **Azure SQL**: `sys.identity_columns.last_value`, `sys.sequences` current value. Compute max value for type (e.g. INT max) and compare.
