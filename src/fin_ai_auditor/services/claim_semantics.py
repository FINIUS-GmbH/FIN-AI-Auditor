from __future__ import annotations

import re
from typing import Final


_WHITESPACE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\s+")

_APPROVAL_REQUIRED_PATTERNS: Final[tuple[str, ...]] = (
    "approval",
    "approve",
    "freigabe",
    "review",
    "gated",
    "guarded",
    "allowlist",
)
_APPROVAL_BYPASS_PATTERNS: Final[tuple[str, ...]] = (
    "without approval",
    "without review",
    "ohne freigabe",
    "ohne review",
    "ungated",
    "direct write",
    "direct persist",
    "auto persist",
)
_NEGATION_MARKERS: Final[tuple[str, ...]] = ("kein ", "keine ", "keinen ", "ohne ", "no ", "not ", "nicht ")
_READ_ONLY_PATTERNS: Final[tuple[str, ...]] = ("read only", "read-only", "readonly", "nur lesend")
_WRITE_ENABLED_PATTERNS: Final[tuple[str, ...]] = (
    "write",
    "persist",
    "save",
    "create",
    "update",
    "patch",
    "delete",
)
_LIFECYCLE_MARKERS: Final[dict[str, tuple[str, ...]]] = {
    "review_status": ("review", "in review", "review-status", "review status"),
    "approved_status": ("approved", "freigegeben", "released"),
    "draft_status": ("draft", "entwurf"),
    "archived_status": ("archived", "archive", "historisiert", "historic"),
}
_SCOPE_MARKERS: Final[dict[str, tuple[str, ...]]] = {
    "tenant_scoped": ("tenant", "project scoped", "project-specific", "projekt"),
    "global_scoped": ("global", "global read", "globaler read"),
}
_REFERENCE_TAGS: Final[frozenset[str]] = frozenset(
    {
        "metamodel_reference",
        "documentation_reference",
        "implementation_reference",
    }
)
_REFERENCE_STOP_MARKERS: Final[tuple[str, ...]] = (
    " is ",
    " sind ",
    " must ",
    " soll ",
    " should ",
    " with ",
    " mit ",
    " has ",
    " hat ",
    " contains ",
    " enthaelt ",
    " which ",
    " wobei ",
)
_REFERENCE_STOP_CHARS: Final[tuple[str, ...]] = (".", ";", ",", "(", ")", "[", "]")
_MEANINGFUL_SEMANTIC_TAGS: Final[frozenset[str]] = frozenset(
    {
        "approval_required",
        "approval_bypass",
        "read_only",
        "write_enabled",
        "read_operation",
        "review_status",
        "approved_status",
        "draft_status",
        "archived_status",
        "tenant_scoped",
        "global_scoped",
        "process_reference",
        "phase_reference",
        "question_reference",
    }
)


def normalize_claim_value(value: str) -> str:
    lowered = str(value or "").strip().casefold().replace("_", " ").replace("-", " ")
    return _WHITESPACE_PATTERN.sub(" ", lowered)


def semantic_consensus_bucket(*, subject_key: str, predicate: str, value: str) -> str:
    signature = semantic_signature_for_claim(subject_key=subject_key, predicate=predicate, value=value)
    meaningful_signature = _meaningful_signature(signature)
    if meaningful_signature:
        return f"semantic:{'|'.join(meaningful_signature)}"
    return f"text:{normalize_claim_value(value)}"


def package_scope_key(subject_key: str) -> str:
    raw_key = str(subject_key or "").strip()
    if not raw_key:
        return "General"
    if raw_key.startswith("EvidenceChain."):
        return "EvidenceChain"
    if raw_key.startswith("TemporalConsistency."):
        return "TemporalConsistency"
    if raw_key.startswith("BSM.phase."):
        return ".".join(raw_key.split(".")[:3])
    if raw_key.startswith("BSM.process"):
        return "BSM.process"
    parts = raw_key.split(".")
    if len(parts) >= 2 and parts[1] in {
        "read_path",
        "write_path",
        "lifecycle",
        "policy",
        "review_status",
        "approval_policy",
        "scope_policy",
    }:
        return parts[0]
    return raw_key


def semantic_signature_for_claim(*, subject_key: str, predicate: str, value: str) -> tuple[str, ...]:
    normalized = normalize_claim_value(value)
    tags: set[str] = set()

    if subject_key.endswith((".write_path", ".policy", ".approval_policy", ".scope_policy")) or "write" in predicate or "policy" in predicate:
        approval_bypass = _contains_any(normalized, _APPROVAL_BYPASS_PATTERNS)
        negated_bypass = approval_bypass and _looks_like_bypass_is_forbidden(normalized_value=normalized)
        if _contains_any(normalized, _APPROVAL_REQUIRED_PATTERNS) or negated_bypass:
            tags.add("approval_required")
        if approval_bypass and not negated_bypass:
            tags.add("approval_bypass")
        if _contains_any(normalized, _READ_ONLY_PATTERNS):
            tags.add("read_only")
        if _contains_any(normalized, _WRITE_ENABLED_PATTERNS):
            tags.add("write_enabled")

    if subject_key.endswith(".read_path") or "read" in predicate:
        if _contains_any(normalized, _READ_ONLY_PATTERNS):
            tags.add("read_only")
        if "read" in normalized or "query" in normalized or "fetch" in normalized or "load" in normalized:
            tags.add("read_operation")

    if subject_key.endswith((".lifecycle", ".review_status")) or "lifecycle" in predicate or "review_status" in predicate:
        for marker, patterns in _LIFECYCLE_MARKERS.items():
            if _contains_any(normalized, patterns):
                tags.add(marker)

    for marker, patterns in _SCOPE_MARKERS.items():
        if _contains_any(normalized, patterns):
            tags.add(marker)

    if subject_key == "BSM.process" or subject_key.startswith("BSM.process.") or subject_key.startswith("BSM.phase."):
        tags.add("process_reference")
        phase_reference = _phase_reference_tag(
            subject_key=subject_key,
            predicate=predicate,
            normalized_value=normalized,
        )
        if phase_reference is not None:
            tags.add(phase_reference)
            tags.add("phase_reference")
        question_reference = _question_reference_tag(
            subject_key=subject_key,
            predicate=predicate,
            normalized_value=normalized,
        )
        if question_reference is not None:
            tags.add(question_reference)
            tags.add("question_reference")
        phase_order = _phase_order_tag(normalized_value=normalized, predicate=predicate)
        if phase_order is not None:
            tags.add(phase_order)
        phase_count = _phase_count_tag(normalized_value=normalized, predicate=predicate)
        if phase_count is not None:
            tags.add(phase_count)
        question_count = _question_count_tag(normalized_value=normalized, predicate=predicate)
        if question_count is not None:
            tags.add(question_count)

    if "metamodel" in normalized or "metamodell" in normalized:
        tags.add("metamodel_reference")
    if "confluence" in normalized or "wiki" in normalized or "doku" in normalized:
        tags.add("documentation_reference")
    if "code" in normalized or "service" in normalized or "router" in normalized:
        tags.add("implementation_reference")

    return tuple(sorted(tags))


def semantic_values_conflict(*, subject_key: str, left_values: set[str], right_values: set[str], predicate: str = "") -> bool:
    left_signatures = {
        semantic_signature_for_claim(subject_key=subject_key, predicate=predicate, value=value)
        for value in left_values
    }
    right_signatures = {
        semantic_signature_for_claim(subject_key=subject_key, predicate=predicate, value=value)
        for value in right_values
    }
    if _has_direct_semantic_conflict(left_signatures=left_signatures, right_signatures=right_signatures):
        return True
    if _has_meaningful_semantic_overlap(left_signatures=left_signatures, right_signatures=right_signatures):
        return False
    if _has_meaningful_semantic_context(left_signatures=left_signatures, right_signatures=right_signatures):
        return True
    return left_values.isdisjoint(right_values)


def semantic_values_aligned(*, subject_key: str, left_value: str, right_value: str, predicate: str = "") -> bool:
    left_signature = semantic_signature_for_claim(subject_key=subject_key, predicate=predicate, value=left_value)
    right_signature = semantic_signature_for_claim(subject_key=subject_key, predicate=predicate, value=right_value)
    if _has_direct_semantic_conflict(left_signatures={left_signature}, right_signatures={right_signature}):
        return False
    if _has_meaningful_semantic_overlap(left_signatures={left_signature}, right_signatures={right_signature}):
        return True
    return normalize_claim_value(left_value) == normalize_claim_value(right_value)


def _has_direct_semantic_conflict(
    *,
    left_signatures: set[tuple[str, ...]],
    right_signatures: set[tuple[str, ...]],
) -> bool:
    for left in left_signatures:
        left_tags = set(left)
        for right in right_signatures:
            right_tags = set(right)
            if ("approval_required" in left_tags and "approval_bypass" in right_tags) or (
                "approval_bypass" in left_tags and "approval_required" in right_tags
            ):
                return True
            if ("read_only" in left_tags and "write_enabled" in right_tags) or (
                "write_enabled" in left_tags and "read_only" in right_tags
            ):
                left_is_gated_write = "write_enabled" in left_tags and "approval_required" in left_tags
                right_is_gated_write = "write_enabled" in right_tags and "approval_required" in right_tags
                if not left_is_gated_write and not right_is_gated_write:
                    return True
            lifecycle_tags = {"review_status", "approved_status", "draft_status", "archived_status"}
            left_lifecycle = left_tags.intersection(lifecycle_tags)
            right_lifecycle = right_tags.intersection(lifecycle_tags)
            if left_lifecycle and right_lifecycle and left_lifecycle.isdisjoint(right_lifecycle):
                return True
            if ("tenant_scoped" in left_tags and "global_scoped" in right_tags) or (
                "global_scoped" in left_tags and "tenant_scoped" in right_tags
            ):
                return True
            if _dynamic_semantic_values(left_tags=left_tags, right_tags=right_tags, prefix="phase_order:"):
                return True
            if _dynamic_semantic_values(left_tags=left_tags, right_tags=right_tags, prefix="phase_count:"):
                return True
            if _dynamic_semantic_values(left_tags=left_tags, right_tags=right_tags, prefix="question_count:"):
                return True
            if _phase_or_question_reference_conflict(
                subject_key_tags_left=left_tags,
                subject_key_tags_right=right_tags,
                reference_prefix="phase_reference:",
            ):
                return True
            if _phase_or_question_reference_conflict(
                subject_key_tags_left=left_tags,
                subject_key_tags_right=right_tags,
                reference_prefix="question_reference:",
            ):
                return True
    return False


def _has_meaningful_semantic_context(
    *,
    left_signatures: set[tuple[str, ...]],
    right_signatures: set[tuple[str, ...]],
) -> bool:
    left_meaningful = {
        _meaningful_signature(signature)
        for signature in left_signatures
        if _meaningful_signature(signature)
    }
    right_meaningful = {
        _meaningful_signature(signature)
        for signature in right_signatures
        if _meaningful_signature(signature)
    }
    return bool(left_meaningful and right_meaningful)


def _has_meaningful_semantic_overlap(
    *,
    left_signatures: set[tuple[str, ...]],
    right_signatures: set[tuple[str, ...]],
) -> bool:
    left_meaningful = [
        set(_meaningful_signature(signature))
        for signature in left_signatures
        if _meaningful_signature(signature)
    ]
    right_meaningful = [
        set(_meaningful_signature(signature))
        for signature in right_signatures
        if _meaningful_signature(signature)
    ]
    return any(
        left_tags.intersection(right_tags) or _compatible_policy_tags(left_tags=left_tags, right_tags=right_tags)
        for left_tags in left_meaningful
        for right_tags in right_meaningful
    )


def _contains_any(value: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern in value for pattern in patterns)


def _looks_like_bypass_is_forbidden(*, normalized_value: str) -> bool:
    return any(marker in normalized_value for marker in _NEGATION_MARKERS)


def _meaningful_signature(signature: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        sorted(
            tag
            for tag in signature
            if (
                (tag in _MEANINGFUL_SEMANTIC_TAGS and tag not in _REFERENCE_TAGS)
                or tag.startswith(("phase_reference:", "question_reference:", "phase_order:", "phase_count:", "question_count:"))
            )
        )
    )


def _dynamic_semantic_values(*, left_tags: set[str], right_tags: set[str], prefix: str) -> bool:
    left_values = {tag for tag in left_tags if tag.startswith(prefix)}
    right_values = {tag for tag in right_tags if tag.startswith(prefix)}
    return bool(left_values and right_values and left_values.isdisjoint(right_values))


def _compatible_policy_tags(*, left_tags: set[str], right_tags: set[str]) -> bool:
    left_is_gated_write = "approval_required" in left_tags and "write_enabled" in left_tags
    right_is_gated_write = "approval_required" in right_tags and "write_enabled" in right_tags
    return ("read_only" in left_tags and right_is_gated_write) or ("read_only" in right_tags and left_is_gated_write)


def _phase_or_question_reference_conflict(
    *,
    subject_key_tags_left: set[str],
    subject_key_tags_right: set[str],
    reference_prefix: str,
) -> bool:
    left_references = {tag for tag in subject_key_tags_left if tag.startswith(reference_prefix)}
    right_references = {tag for tag in subject_key_tags_right if tag.startswith(reference_prefix)}
    return bool(left_references and right_references and left_references.isdisjoint(right_references))


def _phase_reference_tag(*, subject_key: str, predicate: str, normalized_value: str) -> str | None:
    if subject_key.startswith("BSM.phase."):
        phase_key = subject_key.split(".")[2]
        return f"phase_reference:{phase_key}"
    match = re.search(
        r"\b(?:phase|prozessphase|bsm phase)\s*[:#-]?\s*([a-z0-9][a-z0-9 _/-]{1,40})",
        normalized_value,
    )
    if match is not None:
        candidate = _slugify(_clean_reference_candidate(match.group(1)))
        return f"phase_reference:{candidate}" if candidate else None
    if "phase_reference" not in predicate:
        return None
    candidate = _slugify(_clean_reference_candidate(normalized_value))
    return f"phase_reference:{candidate}" if candidate else None


def _question_reference_tag(*, subject_key: str, predicate: str, normalized_value: str) -> str | None:
    if ".question." in subject_key:
        return f"question_reference:{subject_key.rsplit('.question.', 1)[1]}"
    match = re.search(r"\b(?:question|frage)\s*[:#-]?\s*([a-z0-9][a-z0-9 _/-]{1,40})", normalized_value)
    if match is not None:
        candidate = _slugify(_clean_reference_candidate(match.group(1)))
        return f"question_reference:{candidate}" if candidate else None
    if "question_reference" not in predicate:
        return None
    candidate = _slugify(_clean_reference_candidate(normalized_value))
    return f"question_reference:{candidate}" if candidate else None


def _phase_order_tag(*, normalized_value: str, predicate: str) -> str | None:
    if "phase_order" in predicate:
        digits = re.findall(r"\d{1,3}", normalized_value)
        if digits:
            return _normalized_numeric_tag(prefix="phase_order", value=digits[0])
    match = re.search(r"\b(?:phase order|order|reihenfolge)\s*[:=]?\s*(\d{1,3})\b", normalized_value)
    if match is None:
        return None
    value = str(match.group(1) or "").strip()
    return _normalized_numeric_tag(prefix="phase_order", value=value)


def _phase_count_tag(*, normalized_value: str, predicate: str) -> str | None:
    if "phase_count" in predicate:
        digits = re.findall(r"\d{1,3}", normalized_value)
        if digits:
            return _normalized_numeric_tag(prefix="phase_count", value=digits[0])
    match = re.search(
        r"\b(\d{1,3})\s*(?:phases|phase|phasen)\b|\b(?:phase count|phasenzahl)\s*[:=]?\s*(\d{1,3})\b",
        normalized_value,
    )
    if match is None:
        return None
    value = str(match.group(1) or match.group(2) or "").strip()
    return _normalized_numeric_tag(prefix="phase_count", value=value)


def _question_count_tag(*, normalized_value: str, predicate: str) -> str | None:
    if "question_count" in predicate:
        digits = re.findall(r"\d{1,3}", normalized_value)
        if digits:
            return _normalized_numeric_tag(prefix="question_count", value=digits[0])
    match = re.search(
        r"\b(\d{1,3})\s*(?:questions|fragen)\b|\b(?:question count|fragenzahl)\s*[:=]?\s*(\d{1,3})\b",
        normalized_value,
    )
    if match is None:
        return None
    value = str(match.group(1) or match.group(2) or "").strip()
    return _normalized_numeric_tag(prefix="question_count", value=value)


def _slugify(value: str) -> str:
    lowered = str(value or "").strip().casefold()
    collapsed = re.sub(r"[^a-z0-9]+", "_", lowered)
    return collapsed.strip("_")


def _clean_reference_candidate(value: str) -> str:
    candidate = str(value or "").strip()
    for stop_char in _REFERENCE_STOP_CHARS:
        marker_index = candidate.find(stop_char)
        if marker_index > 0:
            candidate = candidate[:marker_index].strip(" -:,.")
            break
    lowered = candidate.casefold()
    for marker in _REFERENCE_STOP_MARKERS:
        marker_index = lowered.find(marker)
        if marker_index > 0:
            candidate = candidate[:marker_index].strip(" -:,.")
            break
    return candidate.strip(" -:,.")


def _normalized_numeric_tag(*, prefix: str, value: str) -> str | None:
    digits = re.findall(r"\d{1,3}", str(value or ""))
    if not digits:
        return None
    normalized = str(int(digits[0]))
    return f"{prefix}:{normalized}"
