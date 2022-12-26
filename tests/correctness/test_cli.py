import click
import dataclasses
import json
import pytest
from click.testing import CliRunner
from pathlib import PosixPath, Path
from pytest import approx

from compgraph import Graph
from compgraph.operations import TRow
from examples.run_inverted_index import inverted_index
from examples.run_pmi import pmi
from examples.run_word_count import word_count
from examples.run_yandex_maps import yandex_maps


@dataclasses.dataclass
class CliTestCase:
    name: str
    func: click.BaseCommand
    expected: list[TRow]
    resource: str


DEFAULT_CASES = [
    CliTestCase(
        name='word_count',
        func=word_count,
        expected=[
            {"text": "a", "count": 3},
            {"text": "it", "count": 3},
            {"text": "ebook", "count": 4},
            {"text": "the", "count": 4},
            {"text": "of", "count": 6},
            {"text": "", "count": 6},
        ],
        resource='text_corpus.txt'
    ),
    CliTestCase(
        name='pmi',
        func=pmi,
        expected=[
            {"doc_id": "a_tale_of_two_cities", "text": "charles", "pmi": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "cities",  "pmi": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "dickens", "pmi": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "ebook",   "pmi": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "gutenberg", "pmi": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "project", "pmi": 0.0}
        ],
        resource='text_corpus.txt'
    ),
    CliTestCase(
        name='inverted_index',
        func=inverted_index,
        expected=[
            {"doc_id": "a_tale_of_two_cities", "text": "1994", "tf_idf": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "2009", "tf_idf": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "2016", "tf_idf": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "25", "tf_idf": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "28", "tf_idf": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "98", "tf_idf": 0.0},
            {"doc_id": "a_tale_of_two_cities", "text": "a", "tf_idf": 0.0}
        ],
        resource='text_corpus.txt'
    )

]

data_text = {
    "doc_id": "a_tale_of_two_cities",
    "text": "\ufeffThe Project Gutenberg EBook of A Tale of Two Cities, by Charles Dickens\n\n"
            "This eBook is for the use of anyone anywhere at no cost and with\n"
            "almost no restrictions whatsoever.  You may copy it, give it away or\n"
            "re-use it under the terms of the Project Gutenberg License included\n"
            "with this eBook or online at www.gutenberg.org\n\n\nTitle: A Tale of Two Cities\n"
            "A Story of the French Revolution\n\nAuthor: Charles Dickens\n\nRelease Date: January, 1994 [EBook #98]\n"
            "Posting Date: November 28, 2009\nLast Updated: September 25, 2016\n"
}


@pytest.fixture(scope='session')
def inputfile(tmp_path_factory: pytest.TempPathFactory) -> Path:
    sub_path = tmp_path_factory.mktemp('test_cli')
    inputfile = sub_path / 'test_text_corpus.txt'
    inputfile.write_text(json.dumps(data_text))
    return inputfile


@pytest.mark.parametrize('case', DEFAULT_CASES)
def test_cli(case: CliTestCase, inputfile: Path, tmp_path: PosixPath) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        name = 'test_result_' + case.name + '.txt'
        outputpath = tmp_path / 'test_cli'
        outputpath.mkdir()
        outputfile = outputpath / name

        result = runner.invoke(case.func, [str(outputfile), str(inputfile)])

        rows = list(Graph.rows_from_file(str(outputfile), lambda x: json.loads(x)))
        for row in case.expected:
            assert row in rows
        assert result.exit_code == 0


data_lengths = [
        {"start": [37.84870228730142, 55.73853974696249], "end": [37.8490418381989, 55.73832445777953],
         "edge_id": "8414926848168493057"},
        {"start": [37.524768467992544, 55.88785375468433], "end": [37.52415172755718, 55.88807155843824],
         "edge_id": "5342768494149337085"}
    ]

data_times = [
        {"leave_time": "20171020T112238.723000", "enter_time": "20171020T112237.427000",
         "edge_id": "8414926848168493057"},
        {"leave_time": "20171011T145553.040000", "enter_time": "20171011T145551.957000",
         "edge_id": "8414926848168493057"},
        {"leave_time": "20171020T090548.939000", "enter_time": "20171020T090547.463000",
         "edge_id": "8414926848168493057"},
        {"leave_time": "20171024T144101.879000", "enter_time": "20171024T144059.102000",
         "edge_id": "8414926848168493057"},
        {"leave_time": "20171022T131828.330000", "enter_time": "20171022T131820.842000",
         "edge_id": "5342768494149337085"},
        {"leave_time": "20171014T134826.836000", "enter_time": "20171014T134825.215000",
         "edge_id": "5342768494149337085"},
        {"leave_time": "20171010T060609.897000", "enter_time": "20171010T060608.344000",
         "edge_id": "5342768494149337085"},
        {"leave_time": "20171027T082600.201000", "enter_time": "20171027T082557.571000",
         "edge_id": "5342768494149337085"}
    ]

expected = [
        {"weekday": "Fri", "hour": 8, "speed": approx(62.2322, 0.001)},
        {"weekday": "Fri", "hour": 9, "speed": approx(78.1070, 0.001)},
        {"weekday": "Fri", "hour": 11, "speed": approx(88.9552, 0.001)},
        {"weekday": "Sat", "hour": 13, "speed": approx(100.9690, 0.001)},
        {"weekday": "Sun", "hour": 13, "speed": approx(21.8577, 0.001)},
        {"weekday": "Tue", "hour": 6, "speed": approx(105.3901, 0.001)},
        {"weekday": "Tue", "hour": 14, "speed": approx(41.5145, 0.001)},
        {"weekday": "Wed", "hour": 14, "speed": approx(106.4505, 0.001)}
    ]


@pytest.fixture(scope='session')
def inputfile_yandex_map(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    sub_path = tmp_path_factory.mktemp('test_cli')
    inputfile_time = sub_path / 'test_travel_times.txt'
    with open(inputfile_time, 'w') as f:
        for row_time in data_times:
            print(json.dumps(row_time), file=f)

    inputfile_length = sub_path / 'test_road_graph_data.txt'
    with open(inputfile_length, 'w') as f:
        for row_length in data_lengths:
            print(json.dumps(row_length), file=f)

    return inputfile_time, inputfile_length


def test_cli_yandex_maps(tmp_path: PosixPath, inputfile_yandex_map: tuple[Path, Path]) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        sub_path = tmp_path / 'test_cli'
        sub_path.mkdir()
        inputfile_time = inputfile_yandex_map[0]
        inputfile_length = inputfile_yandex_map[1]

        outputfile = sub_path / 'test_result_yandex_maps.txt'
        result = runner.invoke(yandex_maps, [str(outputfile), str(inputfile_time), str(inputfile_length)])
        rows = list(Graph.rows_from_file(str(outputfile), lambda x: json.loads(x)))
        for row in expected:
            assert row in rows
        assert result.exit_code == 0


def test_cli_yandex_maps_with_graphic(tmp_path: PosixPath, inputfile_yandex_map: tuple[Path, Path]) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        sub_path = tmp_path / 'test_cli'
        sub_path.mkdir()
        inputfile_time = inputfile_yandex_map[0]
        inputfile_length = inputfile_yandex_map[1]

        outputfile = sub_path / 'test_result_yandex_maps.txt'
        graphicfile = sub_path / 'graphic.png'
        result = runner.invoke(yandex_maps, [str(outputfile), str(inputfile_time), str(inputfile_length),
                                             '-g', str(graphicfile)])
        rows = list(Graph.rows_from_file(str(outputfile), lambda x: json.loads(x)))
        for row in expected:
            assert row in rows
        assert graphicfile.exists()
        assert result.exit_code == 0


def test_cli_help_yandex_maps() -> None:

    runner = CliRunner()
    result = runner.invoke(yandex_maps, ['--help'])
    assert 'yandex-maps' in result.output
    assert '--graphic' in result.output
    assert result.exit_code == 0
