import json
import pandas as pd
import typing as tp
from abc import abstractmethod, ABC
from pathlib import Path

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]
TRowsNext = tp.Iterator[tp.Any] | tp.Generator[TRow, None, None]


class Operation(ABC):
    """Base class"""

    @abstractmethod
    def __call__(self, rows: TRowsGenerator, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    """Read rows from file"""

    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    """Read rows from iterator passed by kwargs"""

    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


class ReadTxtToCSV:
    """Create csv file full rows from file"""

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def __call__(self) -> str:
        data_lst = []
        for row in Read(self.filename, lambda x: json.loads(x))():
            data_lst.append(row)
        name = Path(self.filename).stem
        parent = Path(self.filename).parent
        new_file_name = str(parent) + name + '.csv'
        pd.DataFrame(data_lst).to_csv(new_file_name, index=False)
        return new_file_name


__all__ = ['Operation', 'Read', 'ReadIterFactory', 'ReadTxtToCSV',
           'TRow', 'TRowsIterable', 'TRowsGenerator', 'TRowsNext']
