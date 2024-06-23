/open PRINTING

String graph = System.getProperty("graph")
println("Loading graph " + graph)

import org.commoncrawl.webgraph.explore.Graph
import org.commoncrawl.webgraph.explore.GraphExplorer
import it.unimi.dsi.webgraph.ImmutableGraph

GraphExplorer e = new GraphExplorer(graph)
Graph g = e.getGraph()

println()
println("Graph " + graph + " loaded into GraphExplorer *e*")
println("Type \"e.\" and press <TAB> to list the public methods of the class GraphExplorer")
println("... or \"g.\" for the graph loaded for exploration")

/* Define commands provided by pywebgraph (cn, pwn, ls, sl) */
void cn(String vertexLabel) { e.cn(vertexLabel); }
void cn(long vertexID) { e.cn(vertexID); }
void pwn() { e.pwn(); }
void ls() { e.ls(); }
void ls(long vertexId) { e.ls(vertexId); }
void ls(String vertexLabel) { e.ls(vertexLabel); }
void sl() { e.sl(); }
void sl(long vertexId) { e.sl(vertexId); }
void sl(String vertexLabel) { e.sl(vertexLabel); }

println()
println("... or use one of the predefined methods:")
/methods cn pwn ls sl
println()