# Behavior Validations

Behavior validations compare **runtime or behavioral** aspects: identity/sequence definitions, collation/encoding, triggers, routines (procedures/functions), extended properties (e.g. descriptions), and identity collision risk. They use catalog metadata and optionally run lightweight queries.

## Categories

| Category | Purpose |
|----------|--------|
| [identity_sequence](identity_sequence.md) | Compare identity column and sequence definitions (seed, increment, current value). |
| [collation_encoding](collation_encoding.md) | Compare database and column collation (and encoding where applicable). |
| [triggers](triggers.md) | Compare trigger presence and definition body. |
| [routines](routines.md) | Compare stored procedures and functions (presence and definition). |
| [extended_properties](extended_properties.md) | Compare MS_Description (table/column description) between source and target. |
| [identity_collision_check](identity_collision_check.md) | Assess risk of identity/sequence value collision after migration. |

## Inputs (common)

- **source_schema**, **target_schema** (optional).
- **object_types**: Used for routines (PROCEDURE, FUNCTION); other categories may use TABLE or full catalog.

## Output

Each category produces rows conforming to the [unified output schema](../README.md#unified-output-schema-per-row). **DetailsJson** is category-specific.
