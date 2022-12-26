import click
import json

import compgraph.algorithms as al
from compgraph import Graph


@click.command()
@click.argument('output_filepath', type=click.Path(), default='result_word_count.txt')
@click.argument('input_filepath', type=click.Path(exists=True), default='../resources/text_corpus.txt')
def word_count(input_filepath: str, output_filepath: str) -> None:
    """
    Constructs graph which count words in text
    """
    graph = al.word_count_graph(input_stream_name='input')
    result = graph.run(input=lambda: Graph.rows_from_file(input_filepath, lambda x: json.loads(x)))
    with open(output_filepath, 'w') as out:
        for row in result:
            print(json.dumps(row), file=out)


if __name__ == '__main__':
    word_count()
