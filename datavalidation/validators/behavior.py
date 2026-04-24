"""
Behavior validations: identity/sequence, identity collision, collation, triggers, routines, extended properties.
"""
from datavalidation.results import ValidationResult
from datavalidation.validators.base import BaseValidator


class BehaviorValidator(BaseValidator):
    """Runs all behavior-level validations."""

    def validate_identity_sequence(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Identity columns and sequences. Stub."""
        return ValidationResult(
            validation_name="identity_sequence",
            passed=True,
            summary="Identity & sequence: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_identity_collision(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Next identity vs child FK. Stub."""
        return ValidationResult(
            validation_name="identity_collision",
            passed=True,
            summary="Identity collision check: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_collation(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Collation/encoding. Stub."""
        return ValidationResult(
            validation_name="collation",
            passed=True,
            summary="Collation: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_triggers(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Trigger presence/definitions. Stub."""
        return ValidationResult(
            validation_name="triggers",
            passed=True,
            summary="Triggers: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_routines(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Stored procedures and functions. Stub."""
        return ValidationResult(
            validation_name="routines",
            passed=True,
            summary="Routines: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def validate_extended_properties(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> ValidationResult:
        """Table/column descriptions. Stub."""
        return ValidationResult(
            validation_name="extended_properties",
            passed=True,
            summary="Extended properties: not yet implemented (stub).",
            details=[],
            stats={},
        )

    def run_all(
        self,
        source_schema: str | None = None,
        target_schema: str | None = None,
    ) -> dict[str, ValidationResult]:
        """Run all behavior validations."""
        return {
            "identity_sequence": self.validate_identity_sequence(source_schema, target_schema),
            "identity_collision": self.validate_identity_collision(source_schema, target_schema),
            "collation": self.validate_collation(source_schema, target_schema),
            "triggers": self.validate_triggers(source_schema, target_schema),
            "routines": self.validate_routines(source_schema, target_schema),
            "extended_properties": self.validate_extended_properties(source_schema, target_schema),
        }
