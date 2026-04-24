"""
Rule engine: classify validation outcomes as error, warning, or ignore
based on configurable rules (e.g. env or options).
"""
import os
from typing import Literal

RuleLevel = Literal["error", "warning", "ignore"]


def _parse_rules(env_var: str) -> list[tuple[str, RuleLevel]]:
    """Parse rules from env like RULE_TYPE:PATTERN:MATCH_TYPE (warning|ignore)."""
    raw = os.environ.get(env_var, "").strip()
    if not raw:
        return []
    result = []
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            tokens = part.split(":", 2)
            if len(tokens) >= 3:
                _, pattern, level = tokens[0].strip(), tokens[1].strip(), tokens[2].strip().lower()
                if level in ("warning", "ignore", "error"):
                    result.append((pattern, level))
    return result


def get_rule_level(
    rule_type: str,
    pattern: str,
    default: RuleLevel = "error",
    env_var_prefix: str = "DV_",
) -> RuleLevel:
    """
    Resolve rule level for (rule_type, pattern).
    Env vars: DV_DTYPE_RULES, DV_DEFAULT_VALUE_RULES, DV_INDEX_RULES, DV_FK_RULES.
    """
    env_key = f"{env_var_prefix}{rule_type.upper()}_RULES"
    rules = _parse_rules(env_key)
    for p, level in rules:
        if p in pattern or pattern in p:
            return level
    return default
