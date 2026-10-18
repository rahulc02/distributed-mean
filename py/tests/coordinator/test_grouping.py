import pytest

from dmean.coordinator.grouping import plan_groups
from dmean.coordinator.model import Ready


def ready(*counts, level=1):
    return [Ready(f"p{level}.{i}", f"k{level}.{i}", c, level) for i, c in enumerate(counts)]


def test_groups_exactly_k_and_leaves_the_rest():
    groups = plan_groups(ready(5, 5, 5, 5, 5, 5, 5), f=100, k=5)
    assert len(groups) == 1
    assert len(groups[0].inputs) == 5
    assert not groups[0].final


def test_fewer_than_k_wait_while_files_are_still_in_flight():
    assert plan_groups(ready(5, 5, 5), f=100, k=5) == []


def test_tail_rule_groups_the_remainder_once_counts_reach_f():
    groups = plan_groups(ready(5, 5, 2), f=12, k=5)
    assert len(groups) == 1 and groups[0].final
    assert sum(o.count for o in groups[0].inputs) == 12


def test_a_full_group_covering_every_file_is_final():
    (group,) = plan_groups(ready(2, 2, 2, 2, 2), f=10, k=5)
    assert group.final


def test_a_single_ready_operand_is_never_grouped():
    assert plan_groups(ready(12), f=12, k=5) == []


def test_higher_levels_are_grouped_first():
    pool = ready(1, 1, 1, level=1) + ready(5, 5, level=3)
    first, *_ = plan_groups(pool, f=100, k=2)
    assert {o.level for o in first.inputs} == {3}


def test_k_below_two_is_rejected():
    with pytest.raises(ValueError):
        plan_groups(ready(1, 1), f=2, k=1)
