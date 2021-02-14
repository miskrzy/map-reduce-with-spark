map-reduce-with-spark
using spark and map-reduce approach, the programm calculates some characteristics for a big graph (~300k nodes))

graph was taken from: https://snap.stanford.edu/data/web-Stanford.html
number of nodes: 281903
number of edges: 2312497

characteristics calculated:
 - inner degree of all nodes
 - outer degree of all nodes
 - clustering coefficient for all nodes
 
To run the program:
 - put the graph-file from https://snap.stanford.edu/data/web-Stanford.html to resources directory
 - load maven dependencies
 - run the main function
