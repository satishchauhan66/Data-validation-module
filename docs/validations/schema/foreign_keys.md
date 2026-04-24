# Schema: Foreign Keys

## Purpose

Compare **foreign key** definitions between source and target for matched tables:
- Presence (FK in source but not target, or vice versa).
- **Referenced table** (and schema).
- **Column pairs** (child column → parent column).
- **ON DELETE** and **ON UPDATE** actions (e.g. NO ACTION, CASCADE, SET NULL).

Optional: normalize action names (e.g. 'C' → CASCADE, 'A' → NO ACTION) and treat equivalent actions as match with optional **warning** instead of error.

## Inputs

- **source_schema**, **target_schema** (optional).
- **object_types**: Typically `["TABLE"]`.
- **Rules**: e.g. "action_equivalent" with match_type warning — treat certain action pairs as equivalent but still report as warning.

## Comparison logic

1. **Table pairs**: Matched tables.
2. **Source FKs**: For each table, list FKs: name, referenced schema/table, list of (child_col, parent_col), delete action, update action.
3. **Target FKs**: Same for target.
4. **Match FKs**: Match by normalized FK name (or by referenced table + column pairs if names differ).
5. **Report**:
   - FK in source but not in target → FK missing in target.
   - FK in target but in source → FK missing in source.
   - Referenced table (or schema) differs → referenced table mismatch.
   - Column pairs differ → FK column pairs mismatch.
   - ON DELETE or ON UPDATE action differs (and not equivalent by rule) → delete/update action mismatch.
6. **Status**: `error` or `warning` (e.g. when action_equivalent rule applies).

## Error codes

| Code | Meaning |
|------|--------|
| FK_MISMATCH | Any of: missing in source/target, referenced table mismatch, column pairs mismatch, action mismatch. |

(ErrorDescription distinguishes: "FK missing in source", "FK missing in target", "Referenced table mismatch", "FK column pairs mismatch", "Delete action mismatch", "Update action mismatch".)

## Output (unified row)

- **ValidationType**: `foreign_keys`
- **Status**: `error` or `warning`
- **ObjectType**: TABLE
- **ElementPath**: e.g. `SourceSchema.SourceTable.FkName`
- **DetailsJson**: JSON object with:
  - `constraint_name`: string  
  - `source_ref_schema`: string | null  
  - `source_ref_table`: string | null  
  - `destination_ref_schema`: string | null  
  - `destination_ref_table`: string | null  
  - `source_delete_action`: string | null  
  - `destination_delete_action`: string | null  
  - `source_update_action`: string | null  
  - `destination_update_action`: string | null  
  - `source_column_pairs`: string (e.g. "col1->refcol1,col2->refcol2") | null  
  - `destination_column_pairs`: string | null  

## DB-specific notes

- **DB2**: FK info from SYSCAT.REFERENCES or SYSCAT.TABCONST + referential constraints; delete/update rule codes.
- **Azure SQL**: `sys.foreign_keys`, `sys.foreign_key_columns`; referenced_object_id; delete_referential_action, update_referential_action (e.g. 0=NO_ACTION, 1=CASCADE).
