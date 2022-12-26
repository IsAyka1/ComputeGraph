import typing as tp
from abc import abstractmethod, ABC
from copy import deepcopy
from string import punctuation

from .base import Operation, TRow, TRowsGenerator, TRowsIterable


class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    """Operation for every row"""

    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


# Mappers


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].translate({ord(c): None for c in punctuation})
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].lower()
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        if separator is None:
            self.separator = lambda x: x.isspace()
        else:
            self.separator = lambda x: x == separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        start = 0
        for i in range(len(row[self.column])):
            if self.separator(row[self.column][i]):
                elem = row[self.column][start:i]
                new_row = deepcopy(row)
                new_row[self.column] = elem
                yield new_row
                start = i + 1
        if start != len(row[self.column]):
            elem = row[self.column][start:len(row[self.column])]
            new_row = deepcopy(row)
            new_row[self.column] = elem
            yield new_row


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        res: int | float = row[self.columns[0]]
        for i in range(1, len(self.columns)):
            res *= row[self.columns[i]]
        row.update({self.result_column: res})
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {col: row[col] for col in self.columns}


class Apply(Mapper):
    """Apply function for row"""

    def __init__(self, func: tp.Callable[[TRow], tp.Any], result_column: str | None = None) -> None:
        """
        :param func: function to apply
        :param result_column: name for result column
        """
        self.func = func
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.result_column is None:
            self.func(row)
        else:
            row[self.result_column] = self.func(row)
        yield row


__all__ = ['Map', 'Mapper', 'DummyMapper', 'Filter', 'LowerCase', 'FilterPunctuation', 'Project', 'Product', 'Split',
           'Apply']
