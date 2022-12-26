import copy
import dataclasses
import pytest
import typing as tp
from pytest import approx

from compgraph import operations as ops


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = args

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


def func_edit_row(row: ops.TRow) -> None:
    row['text'], row['test_id'] = str(row['test_id']), row['text']


def func_find_div(row: ops.TRow) -> float:
    return row['number_1'] / row['number_2']


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    mapper_item: int = 0
    mapper_ground_truth_items: tuple[int, ...] = (0,)


MAP_CASES = [
    MapCase(
        mapper=ops.Apply(func=lambda x: x['text'][::-1], result_column='reversed'),
        data=[
            {'test_id': 1, 'text': 'one two three'},
            {'test_id': 2, 'text': 'testing out stuff'}
        ],
        ground_truth=[
            {'test_id': 1, 'text': 'one two three', 'reversed': 'eerht owt eno'},
            {'test_id': 2, 'text': 'testing out stuff', 'reversed': 'ffuts tuo gnitset'}
        ],
        cmp_keys=('test_id', 'text')
    ),
    MapCase(
        mapper=ops.Apply(func=func_edit_row),
        data=[
            {'test_id': 1, 'text': 'one two three'},
            {'test_id': 2, 'text': 'testing out stuff'}
        ],
        ground_truth=[
            {'test_id': 'one two three', 'text': '1'},
            {'test_id': 'testing out stuff', 'text': '2'}
        ],
        cmp_keys=('test_id', 'text')
    ),
    MapCase(
        mapper=ops.Apply(func=func_find_div, result_column='div'),
        data=[
            {'test_id': 1, 'number_1': 5, 'number_2': 15},
            {'test_id': 2, 'number_1': 5, 'number_2': 10},
            {'test_id': 3, 'number_1': 258_963_479, 'number_2': 679}
        ],
        ground_truth=[
            {'test_id': 1, 'number_1': 5, 'number_2': 15, 'div': approx(0.333, abs=0.001)},
            {'test_id': 2, 'number_1': 5, 'number_2': 10, 'div': approx(0.5)},
            {'test_id': 3, 'number_1': 258_963_479, 'number_2': 679, 'div': approx(381_389.513)}
        ],
        cmp_keys=('test_id', 'text')
    )
]


@pytest.mark.parametrize('case', MAP_CASES)
def test_mapper(case: MapCase) -> None:
    mapper_data_row = copy.deepcopy(case.data[case.mapper_item])
    mapper_ground_truth_rows = [copy.deepcopy(case.ground_truth[i]) for i in case.mapper_ground_truth_items]

    key_func = _Key(*case.cmp_keys)

    mapper_result = case.mapper(mapper_data_row)
    assert isinstance(mapper_result, tp.Iterator)
    assert sorted(mapper_ground_truth_rows, key=key_func) == sorted(mapper_result, key=key_func)

    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(case.ground_truth, key=key_func) == sorted(result, key=key_func)
