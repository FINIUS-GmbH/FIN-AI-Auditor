from __future__ import annotations

from tests.forensic_reference_catalog import FORENSIC_FINDING_CLASSES


def test_forensic_reference_catalog_has_unique_class_ids() -> None:
    class_ids = [item.class_id for item in FORENSIC_FINDING_CLASSES]
    assert len(class_ids) == len(set(class_ids))


def test_forensic_reference_catalog_covers_all_priorities_and_strengths() -> None:
    priorities = {item.priority for item in FORENSIC_FINDING_CLASSES}
    strengths = {item.current_strength for item in FORENSIC_FINDING_CLASSES}

    assert priorities == {"P0", "P1"}
    assert strengths == {"strong", "medium", "weak"}


def test_forensic_reference_catalog_contains_all_core_classes() -> None:
    class_ids = {item.class_id for item in FORENSIC_FINDING_CLASSES}
    assert class_ids == {
        "F01",
        "F02",
        "F03",
        "F04",
        "F05",
        "F06",
        "F07",
        "F08",
        "F09",
        "F10",
        "F11",
        "F12",
    }


def test_forensic_reference_catalog_points_to_target_modules() -> None:
    for item in FORENSIC_FINDING_CLASSES:
        assert item.target_modules
        assert all(module for module in item.target_modules)
