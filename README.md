## Compute Graph
`computation graphs` `map` `reduce` `join` `sort` `MapReduce`

### Project Description
This is a library for calculation on graphs.

Calculations on tables are run by _computational graphs_. 
By computational graph we mean a predefined sequence of operations, which can then be applied to various data sets.
```
Table - is a sequence of dictionaries, where each dictionary is a row of the table,
and the dictionary key is a column of the table
```
For simplicity, we can assume that all rows in the input tables contain the same set of keys.

#### Why do we need computational graphs at all?
Computational graphs allow you to separate the description of a sequence of operations from their execution. 
Thanks to this, you can both run operations in another environment (for example, describe a graph in a python interpreter,
and then execute it on a video card), and independently and in parallel run on multiple machines of a computing cluster
to process a large array of input data in an adequate finite time

### Interface
The calculation graph consists of data entry points and operations on them.
#### Entry
* graph_from_iter
```commandline
graph = Graph.graph_from_iter('input')
```
* rows_from_file
```commandline
iter_of_rows = Graph.rows_from_file(filename, parser)
```
* graph_copy
```commandline
another_graph = Graph.graph_copy(graph)
```
#### Operations
* Map - 
The operation, which takes one row and return one row
* Reduce - 
The operation, which takes some rows grouped by keys and returns some rows
* Join - 
The operation, which join two graphs into one
* Sort - 
The operation, which sort rows by keys

#### Run
After the description of the graph, you need to run.
```commandline
graph.run(input=lambda: iter([{'key': 'value'}]))
```
or
```commandline
graph.run(input=lambda: Graph.rows_from_file(filename, parser)))
```
You can run graph anytimes with different input.
### Installing
```commandline
pip install compgraph
```
### How to use
Create a graph from stack of operations then run it from using your own data
```commandline
graph = Graph().operation1(...)
               .operation2(...)
               .operation2(...)
result = graph.run()
```
#### Scripts
You can use `python run_TASK` if you're going from 'examples' path. You should unarchive 'extract_me.tgz' before.

With dafault resources it can take some time.
* word_count

Constructs graph which count words in text
```commandline
python run_word_count [OUTPUT_FILENAME] [INPUT_FILENAME]
```
* inverted_index

Constructs graph which calculates td-idf for every word/document pair top N(3)
```commandline
python run_inverted_index [OUTPUT_FILENAME] [INPUT_FILENAME] [-n INT]
```
* pmi

Constructs graph which gives for every document the top N(10) words ranked by pointwise mutual information
```commandline
python run_pmi [OUTPUT_FILENAME] [INPUT_FILENAME] [-n INT]
```
* yandex_maps

Constructs graph which measures average speed in km/h depending on the weekday and hour
```commandline
python run_pmi [OUTPUT_FILENAME] [INPUT_TIME_FILENAME] [INPUT_LENGTH_FILENAME] [--graphic PATH]
```
You can create heatmap graphic with _--graphic/-g_ option
### Testing
```commandline
pytest compgraph
```
