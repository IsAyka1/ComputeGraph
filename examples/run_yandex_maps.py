import click
import json
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import compgraph.algorithms as al
from compgraph import Graph
from compgraph.operations import TRowsIterable


@click.command()
@click.option('-g', '--graphic', type=click.Path(), default='', help='Path where to create a heatmap')
@click.argument('output_filepath', type=click.Path(), default='result_yandex_maps.txt')
@click.argument('input_time_filepath', type=click.Path(exists=True), default='../resources/travel_times.txt')
@click.argument('input_length_filepath', type=click.Path(exists=True), default='../resources/road_graph_data.txt')
def yandex_maps(input_time_filepath: str, input_length_filepath: str, output_filepath: str, graphic: str) -> None:
    """
    Constructs graph which measures average speed in km/h depending on the weekday and hour
    """
    graph = al.yandex_maps_graph(input_stream_name_time='input_time',
                                 input_stream_name_length='input_length')
    result: TRowsIterable = graph.run(
        input_time=lambda: Graph.rows_from_file(input_time_filepath, lambda x: json.loads(x)),
        input_length=lambda: Graph.rows_from_file(input_length_filepath, lambda x: json.loads(x))
    )

    if graphic:
        result = list(result)
        create_heatmap(result, graphic)
    with open(output_filepath, 'w') as out:
        for row in result:
            print(json.dumps(row), file=out)


def create_heatmap(rows: TRowsIterable, output_filepath: str = 'heatmap_yandex_maps.png') -> None:

    df = pd.DataFrame(rows)
    df_matrix = pd.DataFrame(index=[pd.unique(df['weekday'].values)], columns=[pd.unique(df['hour'].values)])\
        .fillna(value=0)

    for row in df.values:
        hour = row[1]
        weekday = row[0]
        speed = row[-1]
        df_matrix.at[weekday, hour] = speed

    plt.figure(figsize=(24, 7))
    heatmap = sns.heatmap(df_matrix, annot=True)
    heatmap.set(xlabel='Hours', ylabel='Weekdays')
    heatmap.set_title('Correlation Heatmap of speed', fontdict={'fontsize': 12}, pad=12)
    plt.savefig(output_filepath, dpi=300, bbox_inches='tight')


if __name__ == '__main__':
    yandex_maps()
