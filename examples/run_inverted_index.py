import click
import json

import compgraph.algorithms as al
from compgraph import Graph


@click.command()
@click.option('-n', default=10, help='Top N docs for every word')
@click.argument('output_filepath', type=click.Path(), default='result_inverted_index.txt')
@click.argument('input_filepath', type=click.Path(exists=True), default='../resources/text_corpus.txt')
def inverted_index(input_filepath: str, output_filepath: str, n: int) -> None:
    """
    Constructs graph which calculates td-idf for every word/document pair top N(3)
    """
    graph = al.inverted_index_graph(input_stream_name='input', n=n)
    result = graph.run(input=lambda: Graph.rows_from_file(input_filepath, lambda x: json.loads(x)))
    with open(output_filepath, 'w') as out:
        for row in result:
            print(json.dumps(row), file=out)


if __name__ == '__main__':
    inverted_index()
