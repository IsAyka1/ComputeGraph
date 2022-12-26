import datetime
import math
from calendar import day_name
from numpy import log

from compgraph import Graph, operations


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""

    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def _calculate_td_idf(row: operations.TRow) -> float:
    P_w1 = row['docs']
    P_w2 = row['count_doc']
    return log((P_w1 / P_w2))


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf', n: int = 3) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""

    graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([doc_column, text_column])

    graph_doc_count = graph \
        .reduce(operations.Count('docs'), [doc_column]) \
        .reduce(operations.Count('docs'), [])

    graph_idf = graph \
        .reduce(operations.Count('count'), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count('count_doc'), [text_column]) \
        .join(operations.InnerJoiner(), graph_doc_count, [])

    return graph \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner(), graph_idf, [text_column]) \
        .map(operations.Apply(_calculate_td_idf, result_column)) \
        .map(operations.Product(['tf', result_column], result_column)) \
        .reduce(operations.TopN(result_column, n), [text_column]) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column, text_column])


def _calculate_pmi(row: operations.TRow) -> float:
    P_w1_w2 = row['tf_doc']
    P_w1 = row['tf_sum']
    return log((P_w1_w2 / P_w1))


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi', n: int = 10) -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([doc_column, text_column])

    graph_filtered = graph \
        .reduce(operations.Count('count'), [doc_column, text_column]) \
        .map(operations.Filter(lambda x: x['count'] >= 2 and len(x[text_column]) > 4))

    graph = graph.join(operations.InnerJoiner(), graph_filtered, [doc_column, text_column])

    graph_sum = graph\
        .reduce(operations.TermFrequency(text_column, result_column='tf_sum'), [])\
        .sort([text_column])

    return graph \
        .reduce(operations.TermFrequency(text_column, result_column='tf_doc'), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner(), graph_sum, [text_column]) \
        .map(operations.Apply(_calculate_pmi, result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column, result_column]) \
        .reduce(operations.TopN(result_column, n), [doc_column])


def _find_length(row: operations.TRow) -> float:
    EARTH_RADIUS_KM = 6373
    start = row['start']
    end = row['end']
    lon1, lat1 = start
    lon2, lat2 = end
    lat1_rad, lat2_rad, lon1_rad, lon2_rad = map(lambda x: x * math.pi / 180, [lat1, lat2, lon1, lon2])
    a = (pow(math.sin((lat2_rad - lat1_rad) / 2), 2) +
         pow(math.sin((lon2_rad - lon1_rad) / 2), 2) *
         math.cos(lat1_rad) * math.cos(lat2_rad))
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _get_seconds(row: operations.TRow) -> float:
    for column in 'enter_time', 'leave_time':
        if len(row[column].split('.')) == 2:
            row[column] = datetime.datetime.strptime(row[column], '%Y%m%dT%H%M%S.%f')
        else:
            row[column] = datetime.datetime.strptime(row[column], '%Y%m%dT%H%M%S')
    return (row['leave_time'] - row['enter_time']).total_seconds()


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""

    length_graph = Graph.graph_from_iter(input_stream_name_length) \
        .map(operations.Apply(_find_length, 'length')) \
        .sort([edge_id_column]) \
        .map(operations.Project([edge_id_column, 'length']))

    time_graph = Graph.graph_from_iter(input_stream_name_time) \
        .sort([edge_id_column]) \
        .join(operations.InnerJoiner(), length_graph, [edge_id_column]) \
        .sort([enter_time_column]) \
        .map(operations.Apply(_get_seconds, 'seconds')) \
        .map(operations.Project([enter_time_column, 'seconds', 'length'])) \
        .map(operations.Apply(lambda x: day_name[x[enter_time_column].weekday()][:3], weekday_result_column)) \
        .map(operations.Apply(lambda x: x[enter_time_column].hour, hour_result_column)) \
        .sort([weekday_result_column, hour_result_column])

    graph_length_sum = time_graph \
        .reduce(operations.Sum('length'), [weekday_result_column, hour_result_column])

    return time_graph.reduce(operations.Sum('seconds'), [weekday_result_column, hour_result_column]) \
        .join(operations.InnerJoiner(), graph_length_sum, [weekday_result_column, hour_result_column]) \
        .map(operations.Apply(lambda x: x['length'] / x['seconds'] * 3600, speed_result_column)) \
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))


__all__ = ['word_count_graph', 'inverted_index_graph', 'pmi_graph', 'yandex_maps_graph']
