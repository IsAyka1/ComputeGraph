import click
import json

import compgraph.algorithms as al
from compgraph import Graph


@click.command()
@click.option('-n', default=10, type=int, help='Top N of words')
@click.argument('output_filepath', type=click.Path(), default='result_pmi.txt')
@click.argument('input_filepath', type=click.Path(exists=True), default='../resources/text_corpus.txt')
def pmi(input_filepath: str, output_filepath: str, n: int = 10) -> None:
    """
    Constructs graph which gives for every document the top N(10) words ranked by pointwise mutual information
    """
    graph = al.pmi_graph(input_stream_name='input', n=n)
    result = graph.run(input=lambda: Graph.rows_from_file(input_filepath, lambda x: json.loads(x)))
    with open(output_filepath, 'w') as out:
        for row in result:
            print(json.dumps(row), file=out)


if __name__ == '__main__':
    pmi()
