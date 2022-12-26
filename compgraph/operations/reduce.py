import typing as tp
from abc import abstractmethod, ABC
from collections import defaultdict
from itertools import groupby

from .base import Operation, TRow, TRowsIterable, TRowsGenerator


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    """Operation for rows grouped by keys"""
    def __init__(self, reducer: Reducer, keys: tp.Tuple[str, ...]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for key, group in groupby(rows, lambda x: [x.get(k) for k in self.keys]):
            yield from self.reducer(self.keys, group)


# Reducers


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        yield from rows
        return


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max: str = column
        self.n: int = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        new_rows: list[TRow] = []
        min_tmp: tp.Any = None
        for row in rows:
            if min_tmp is None:
                min_tmp = row[self.column_max]
            if row[self.column_max] > min_tmp and len(new_rows) == self.n or len(new_rows) < self.n:
                if row[self.column_max] > min_tmp and len(new_rows) == self.n:
                    new_rows.remove([row for row in new_rows if row[self.column_max] == min_tmp][0])
                new_rows.append(row)
                min_tmp = min(map(lambda x: x[self.column_max], new_rows))
        new_rows.sort(key=lambda x: x[self.column_max], reverse=True)
        yield from new_rows


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        count = 0
        new_rows = []
        dct: defaultdict[str, int] = defaultdict(int)
        for row in rows:
            count += 1
            dct[row[self.words_column]] += 1
            if dct[row[self.words_column]] == 1:
                new_row = {key: row[key] for key in group_key}
                new_row[self.words_column] = row[self.words_column]
                new_rows.append(new_row)

        for new_row in new_rows:
            word = new_row[self.words_column]
            new_row[self.result_column] = dct[word] / count
            yield new_row


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        count = 0
        new_row: TRow = {}
        for row in rows:
            count += 1
            new_row = new_row or {key: row[key] for key in group_key}
        new_row[self.column] = count
        yield new_row


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        new_row: TRow = {}
        row_sum: int | float = 0
        for row in rows:
            row_sum += row[self.column]
            new_row = new_row or {key: row[key] for key in group_key}
        new_row[self.column] = row_sum
        yield new_row


__all__ = ['Reduce', 'Reducer', 'FirstReducer', 'TopN', 'Sum', 'Count', 'TermFrequency']
