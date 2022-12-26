import typing as tp

from . import operations as ops


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self.operation: ops.Operation = ops.ReadIterFactory('')  # for mypy
        self.parents: list[Graph] | None = None
        self.join_parent: Graph | None = None
        self.input: ops.TRow | None = None

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph()
        graph.operation = ops.ReadIterFactory(name)
        return graph

    @staticmethod
    def rows_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> ops.TRowsGenerator:
        """Read rows from file
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        return ops.Read(filename, parser)()

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        graph = Graph()
        graph.operation = ops.Map(mapper)
        graph.parents = [self]
        return graph

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph = Graph()
        graph.operation = ops.Reduce(reducer, tuple(keys))
        graph.parents = [self]
        return graph

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph = Graph()
        graph.operation = ops.ExternalSort(keys)
        graph.parents = [self]
        return graph

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph = Graph()
        graph.operation = ops.Join(joiner, keys)
        graph.parents = [self, join_graph]
        return graph

    def run(self, **kwargs: tp.Any) -> ops.TRowsGenerator:
        """Single method to start execution; data sources passed as kwargs"""
        self.input = kwargs
        if self.parents is not None:
            streams = [p.run(**self.input) for p in self.parents]
            return self.operation(*streams)
        else:
            return self.operation(**self.input)


__all__ = ['Graph']
