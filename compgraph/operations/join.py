import typing as tp
from abc import abstractmethod, ABC
from copy import deepcopy
from itertools import groupby

from .base import Operation, TRow, TRowsIterable, TRowsGenerator, TRowsNext


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b
        self.same_col: list[str] = []

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsNext, rows_b: TRowsNext) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass


class Join(Operation):
    """Construct graph joined with another graph"""

    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner
        self.join_iter: TRowsNext = iter([])

    def __call__(self, rows: TRowsNext, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        if len(args):
            self.join_iter = args[0]
        a_groupby_gen = groupby(rows, lambda x: [x.get(k) for k in self.keys])
        key_a, group_a = next(a_groupby_gen, (None, iter([])))
        b_groupby_gen = groupby(self.join_iter, lambda x: [x.get(k) for k in self.keys])
        key_b, group_b = next(b_groupby_gen, (None, iter([])))
        while key_a is not None or key_b is not None:
            if key_a == key_b:
                yield from self.joiner(self.keys, group_a, group_b)
                key_a, group_a = next(a_groupby_gen, (None, iter([])))
                key_b, group_b = next(b_groupby_gen, (None, iter([])))
            elif key_b is None or (key_a is not None and key_a < key_b):
                yield from self.joiner(self.keys, group_a, iter([]))
                key_a, group_a = next(a_groupby_gen, (None, iter([])))
            elif key_a is None or (key_b is not None and key_b < key_a):
                yield from self.joiner(self.keys, iter([]), group_b)
                key_b, group_b = next(b_groupby_gen, (None, iter([])))


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        super(InnerJoiner, self).__init__(suffix_a, suffix_b)

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = list(rows_b)
        for row_a in rows_a:
            for row_b in rows_b:
                new_row = deepcopy(row_b)
                for key in row_a.keys():
                    if key in row_b.keys() and key not in keys:
                        new_row[key + self._a_suffix] = row_a[key]
                        new_row[key + self._b_suffix] = row_b[key]
                        new_row.pop(key)
                    else:
                        new_row[key] = row_a[key]
                yield new_row


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        super(OuterJoiner, self).__init__(suffix_a, suffix_b)

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsNext,
                 rows_b: TRowsNext) -> TRowsGenerator:
        try:
            row_b: TRow = next(rows_b)
        except StopIteration:
            for row_a in rows_a:
                yield deepcopy(row_a)
        else:
            rows_b_lst: list[TRow] = list(rows_b)
            rows_b_lst.insert(0, row_b)
            yield from RightJoiner(self._a_suffix, self._b_suffix)(keys, rows_a, rows_b_lst)


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        super(LeftJoiner, self).__init__(suffix_a, suffix_b)

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsNext, rows_b: TRowsNext) -> TRowsGenerator:
        try:
            row_b: TRow = next(rows_b)
        except StopIteration:
            for row_a in rows_a:
                yield deepcopy(row_a)
        else:
            rows_b_lst: list[TRow] = list(rows_b)
            rows_b_lst.insert(0, row_b)
            yield from InnerJoiner(self._a_suffix, self._b_suffix)(keys, rows_a, rows_b_lst)


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        super(RightJoiner, self).__init__(suffix_a, suffix_b)

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsNext, rows_b: TRowsIterable)\
            -> TRowsGenerator:
        try:
            row_a: TRow = next(rows_a)
        except StopIteration:
            for row_b in rows_b:
                yield deepcopy(row_b)
        else:
            rows_a_lst: list[TRow] = list(rows_a)
            rows_a_lst.insert(0, row_a)
            yield from InnerJoiner(self._a_suffix, self._b_suffix)(keys, rows_b, rows_a_lst)


__all__ = ['Join', 'Joiner', 'InnerJoiner', 'OuterJoiner', 'RightJoiner', 'LeftJoiner']
