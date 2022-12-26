import dataclasses
import json
import pytest
import typing as tp
from pathlib import PosixPath

from compgraph import Graph, operations


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = args

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


def test_map() -> None:
    input_steam_name = 'input'
    graph = Graph.graph_from_iter(input_steam_name) \
        .map(operations.DummyMapper())
    data = [
       {'test_id': 1, 'text': 'one two three'},
       {'test_id': 2, 'text': 'testing out stuff'}
           ]
    expected = [
       {'test_id': 1, 'text': 'one two three'},
       {'test_id': 2, 'text': 'testing out stuff'}
    ]
    result = graph.run(input=lambda: iter(data))
    assert list(result) == expected

    result_second = graph.run(input=lambda: iter(data))
    assert list(result_second) == expected


def test_reduce() -> None:
    input_steam_name = 'input'
    reducer_keys = ('test_id',)
    graph = Graph.graph_from_iter(input_steam_name) \
        .reduce(operations.FirstReducer(), reducer_keys)
    data = [
       {'test_id': 1, 'text': 'one two three'},
       {'test_id': 2, 'text': 'testing out stuff'}
           ]
    expected = [
       {'test_id': 1, 'text': 'one two three'},
       {'test_id': 2, 'text': 'testing out stuff'}
    ]
    result = graph.run(input=lambda: iter(data))
    assert list(result) == expected

    result_second = graph.run(input=lambda: iter(data))
    assert list(result_second) == expected


def test_join() -> None:
    input_steam_name_right = 'input_right'
    input_steam_name_left = 'input_left'
    join_keys = ('player_id',)
    graph_right = Graph.graph_from_iter(input_steam_name_right) \
        .map(operations.DummyMapper())
    graph_left = Graph.graph_from_iter(input_steam_name_left) \
        .join(operations.InnerJoiner(), graph_right, join_keys)
    data_right = [
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 1, 'score': 22},
        {'game_id': 1, 'player_id': 3, 'score': 99}
    ]
    data_left = [
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'},
        {'player_id': 3, 'username': 'Destroyer'},
    ]
    expected = [
        {'game_id': 1, 'player_id': 3, 'score': 99, 'username': 'Destroyer'},
        {'game_id': 2, 'player_id': 1, 'score': 17, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 1, 'score': 22, 'username': 'XeroX'}
    ]

    key_func = _Key(*join_keys)

    result = graph_left.run(input_right=lambda: iter(data_right),
                            input_left=lambda: iter(data_left))
    assert sorted(result, key=key_func) == sorted(expected, key=key_func)

    result_second = graph_left.run(input_right=lambda: iter(data_right),
                                   input_left=lambda: iter(data_left))
    assert sorted(result_second, key=key_func) == sorted(expected, key=key_func)


@dataclasses.dataclass
class SortCase:
    keys: tp.Sequence[str]
    data: list[operations.TRow]
    expected: list[operations.TRow]


SORT_CASES = [
    SortCase(
        keys=['test_id'],
        data=[
            {'test_id': 2, 'text': 'testing out stuff'},
            {'test_id': 1, 'text': 'w one two three'}
        ],
        expected=[
            {'test_id': 1, 'text': 'w one two three'},
            {'test_id': 2, 'text': 'testing out stuff'}
        ]
    ),
    SortCase(
        keys=['text'],
        data=[
            {'test_id': 2, 'text': 'testing out stuff'},
            {'test_id': 3, 'text': 'hello world'},
            {'test_id': 1, 'text': 'w one two three'}
        ],
        expected=[
            {'test_id': 3, 'text': 'hello world'},
            {'test_id': 2, 'text': 'testing out stuff'},
            {'test_id': 1, 'text': 'w one two three'}
        ]
    )
]


@pytest.mark.parametrize('case', SORT_CASES)
def test_sort(case: SortCase) -> None:
    input_steam_name = 'input'
    graph = Graph.graph_from_iter(input_steam_name).sort(case.keys)

    result = graph.run(input=lambda: iter(case.data))
    assert list(result) == case.expected

    result_second = graph.run(input=lambda: iter(case.data))
    assert list(result_second) == case.expected


def test_rows_from_file(tmp_path: PosixPath) -> None:
    data = [
        {'test_id': 1, 'text': 'one two three'},
        {'test_id': 2, 'text': 'testing out stuff'}
    ]

    sub_path = tmp_path / 'sub'
    sub_path.mkdir()
    file = sub_path / 'data.txt'
    with open(file, 'w') as f:
        for row in data:
            print(json.dumps(row), file=f)

    graph = Graph.rows_from_file(filename=str(file), parser=lambda x: json.loads(x))
    assert list(graph) == data


def test_graph_from_iter() -> None:
    data = [
        {'test_id': 1, 'text': 'one two three'},
        {'test_id': 2, 'text': 'testing out stuff'}
    ]
    graph = Graph.graph_from_iter('input')

    result_graph_from_iter = graph.run(input=lambda: iter(data))
    assert list(result_graph_from_iter) == data
