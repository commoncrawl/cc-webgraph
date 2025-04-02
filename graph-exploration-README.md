# Interactive Graph Exploration

A tutorial how to interactively explore the Common Crawl webgraphs – or other graphs using the webgraph format – using the [JShell](https://docs.oracle.com/en/java/javase/21/jshell/index.html) and the [GraphExplorer](src/main/java/org/commoncrawl/webgraph/explore/GraphExplorer.java) class.


## Quick Start

1. change into the "cc-webgraph" project directory, [build the cc-webgraph JAR](README.md#compiling-and-packaging-java-tools) and remember the project directory and the JAR using environment variables:

   ```
   $> cd .../cc-webgraph

   $> mvn clean package

   $> CC_WEBGRAPH=$PWD
   $> CC_WEBGRAPH_JAR=$PWD/target/cc-webgraph-*-jar-with-dependencies.jar
   ```

2. select a web graph you want to explore, choose a download directory and download the web graph

   ```
   $> GRAPH=cc-main-2024-feb-apr-may-domain

   $> mkdir .../my-webgraphs/$GRAPH
   $> cd .../my-webgraphs/$GRAPH
   ```

   About 15 GiB disk are needed to hold all files of a domain-level webgraph.

   ```
   $> $CC_WEBGRAPH/src/script/webgraph_ranking/graph_explore_download_webgraph.sh $GRAPH
   ```

3. Build the map from vertex label to vertex ID and vice versa. This allows to look up a reverse domain name (e.g. "org.commoncrawl") and get the corresponding vertex ID.

   ```
   $> $CC_WEBGRAPH/src/script/webgraph_ranking/graph_explore_build_vertex_map.sh $GRAPH $GRAPH-vertices.txt.gz
   ```

4. Launch the [JShell](https://docs.oracle.com/en/java/javase/21/jshell/index.html)

   ```
   $> jshell --class-path $CC_WEBGRAPH_JAR
   |  Welcome to JShell -- Version 21.0.3
   |  For an introduction type: /help intro
   
   jshell> 
   ```

   Now you may play around with the JShell or load the GraphExplorer class and your graph:

   ```
   jshell> import org.commoncrawl.webgraph.explore.GraphExplorer
   
   jshell> GraphExplorer e = new GraphExplorer("cc-main-2024-feb-apr-may-domain")
   2024-06-23 13:38:51:084 +0200 [main] INFO Graph - Loading graph cc-main-2024-feb-apr-may-domain.graph
   2024-06-23 13:38:51:193 +0200 [main] INFO Graph - Loading transpose of the graph cc-main-2024-feb-apr-may-domain-t.graph
   2024-06-23 13:38:51:279 +0200 [main] INFO Graph - Loading vertex map cc-main-2024-feb-apr-may-domain.iepm (ImmutableExternalPrefixMap)
   2024-06-23 13:38:52:356 +0200 [main] INFO Graph - Loaded graph cc-main-2024-feb-apr-may-domain.graph
   e ==> org.commoncrawl.webgraph.explore.GraphExplorer@4cc0edeb
   ```

   But for now exit the JShell
   ```
   jshell> /exit
   |  Goodbye
   ```

   To make the loading easier, you may use the load script [graph_explore_load_graph.jsh](src/script/webgraph_ranking/graph_explore_load_graph.jsh) and pass the graph name as a Java property to the JShell via command-line option `-R-Dgraph=$GRAPH`

   ```
   $> jshell --class-path $CC_WEBGRAPH_JAR \
             -R-Dgraph=$GRAPH \
             $CC_WEBGRAPH/src/script/webgraph_ranking/graph_explore_load_graph.jsh
   Loading graph cc-main-2024-feb-apr-may-domain
   2024-06-23 13:30:14:134 +0200 [main] INFO Graph - Loading graph cc-main-2024-feb-apr-may-domain.graph
   2024-06-23 13:30:14:340 +0200 [main] INFO Graph - Loading transpose of the graph cc-main-2024-feb-apr-may-domain-t.graph
   2024-06-23 13:30:14:439 +0200 [main] INFO Graph - Loading vertex map cc-main-2024-feb-apr-may-domain.iepm (ImmutableExternalPrefixMap)
   2024-06-23 13:30:15:595 +0200 [main] INFO Graph - Loaded graph cc-main-2024-feb-apr-may-domain.graph
   
   Graph cc-main-2024-feb-apr-may-domain loaded into GraphExplorer *e*
   Type "e." and press <TAB> to list the public methods of the class GraphExplorer
   ... or "g." for the graph loaded for exploration
   
   ... or use one of the predefined methods:
     void cn(String)
     void cn(long)
     void pwn()
     void ls()
     void ls(long)
     void ls(String)
     void sl()
     void sl(long)
     void sl(String)
   
   |  Welcome to JShell -- Version 21.0.3
   |  For an introduction type: /help intro
   
   jshell> 
   ```

   The predefined methods are those provided by [pyWebGraph](https://github.com/mapio/py-web-graph).

   ```
   jshell> cn("org.commoncrawl")
   #111997321      org.commoncrawl
   
   jshell> pwn()
   #111997321      org.commoncrawl
   
   jshell> ls()  // list successors (vertices linked from the domain commoncrawl.org or one of its subdomains)
   
   jshell> sl()  // list predecessors (vertices connected via incoming links)
   ```


## Using the Java Classes

The Java classes "GraphExplorer" and "Graph" bundle a set of methods which help exploring the graphs:
- load the webgraph, its transpose and the vertex map
- access the vertices and their successors or predecessors
- utilities to import or export a list of vertices or counts from or into a file

The methods are bundled in the classes of the Java package `org.commoncrawl.webgraph.explore`. To get an overview over all provided methods, inspect the source code or see the section [Javadocs](README.md#javadocs) in the main README for how to read the Javadocs. Here only few examples are presented.

We start again with launching the JShell and loading a webgraph:

```
$> jshell --class-path $CC_WEBGRAPH_JAR \
          -R-Dgraph=$GRAPH \
          $CC_WEBGRAPH/src/script/webgraph_ranking/graph_explore_load_graph.jsh
jshell> 
```

Two classes are already instantiated – the *GraphExplorer* `e` and the *Graph* `g`, the former holds a reference to the latter:

```
jshell> /vars
|    String graph = "cc-main-2024-feb-apr-may-domain"
|    GraphExplorer e = org.commoncrawl.webgraph.explore.GraphExplorer@7dc7cbad
|    Graph g = org.commoncrawl.webgraph.explore.Graph@4f933fd1

jshell> e.getGraph()
$45 ==> org.commoncrawl.webgraph.explore.Graph@4f933fd1
```

First, the vertices in the webgraphs are represented by numbers. So, we need to translage between vertex label and ID:

```
jshell> g.vertexLabelToId("org.wikipedia")
$46 ==> 115107569

jshell> g.vertexIdToLabel(115107569)
$47 ==> "org.wikipedia"
```

One important note: Common Crawl's webgraphs list the host or domain names in [reverse domain name notation](https://en.wikipedia.org/wiki/Reverse_domain_name_notation). The vertex lists are sorted by the reversed names in lexicographic order and then numbered continuously. This gives a close-to-perfect compression of the webgraphs itself. Most of the arcs are close in terms of locality because subdomains or sites of the same region (by country-code top-level domain) are listed in one continous block. Cf. the paper [The WebGraph Framework I: Compression Techniques](https://vigna.di.unimi.it/ftp/papers/WebGraphI.pdf) by Paolo Boldi and Sebastiano Vigna.

Now, let's look how many other domains are linked from Wikipedia?

```
jshell> g.outdegree("org.wikipedia")
$46 ==> 2106338
```

Another note: Common Crawl's webgraphs are based on sample crawls of the web. Same as the crawls, also the webgraphs are not complete and the Wikipedia may in reality link to far more domains. But 2 million linked domains is already not a small sample.

The Graph class also gives you access to the successors of a vertex, as array or stream of integers, but also as stream of strings (vertex labels):

```
jshell> g.successors("org.wikipedia").length
$48 ==> 2106338

jshell> g.successorIntStream("org.wikipedia").count()
$49 ==> 2106338

jshell> g.successorStream("org.wikipedia").limit(10).forEach(System.out::println)
abb.global
abb.nic
abbott.cardiovascular
abbott.globalpointofcare
abbott.molecular
abbott.pk
abc.www
abudhabi.gov
abudhabi.mediaoffice
abudhabi.tamm
```

Using Java streams it's easy to translate between the both representations:

```
jshell> g.successorIntStream("org.wikipedia").limit(5).mapToObj(i -> g.vertexIdToLabel(i)).forEach(System.out::println)
abb.global
abb.nic
abbott.cardiovascular
abbott.globalpointofcare
abbott.molecular
```

Successors represent outgoing links to other domains. We can do the same for predecsors, that is incoming links from other domains:

```
jshell> g.indegree("org.wikipedia")
$50 ==> 2752391

jshell> g.predecessorIntStream("org.wikipedia").count()
$51 ==> 2752391

jshell> g.predecessorStream("org.wikipedia").limit(5).forEach(System.out::println)
abogado.fabiobalbuena
abogado.jacksonville
abogado.jaskot
abogado.super
ac.789bet
```

Technically, webgraphs only store successor lists. But the Graph class holds also two graphs: the "original" one and its transpose. In the transposed graph "successors" are "predecessors", and "outdegree" means "indegree". Some methods on a deeper level take one of the two webgraphs as argument, here it makes a difference whether you pass `g.graph` or `g.graphT`, here to a method which translates vertex IDs to labels and extracts the top-level domain:

```
jshell> g.successorTopLevelDomainStream(g.graph, g.vertexLabelToId("org.wikipedia")).limit(5).forEach(System.out::println)
abb
abb
abbott
abbott
abbott

jshell> g.successorTopLevelDomainStream(g.graphT, g.vertexLabelToId("org.wikipedia")).limit(5).forEach(System.out::println)
abogado
abogado
abogado
abogado
ac
```

The top-level domains repeat, and you may want to count the occurrences and create a frequency list. There is a predefined method to perform this:

```
jshell> g.successorTopLevelDomainCounts("org.wikipedia").filter(e -> e.getKey().startsWith("abb")).forEach(e -> System.out.printf("%8d\t%s\n", e.getValue(), e.getKey()))
       4        abbott
       2        abb

jshell> g.successorTopLevelDomainCounts("org.wikipedia").limit(10).forEach(e -> System.out.printf("%8d\t%s\n", e.getValue(), e.getKey()))
  706707        com
  213406        org
  117042        de
   86684        net
   65906        ru
   55914        fr
   53628        uk
   52828        it
   51622        jp
   33729        br
```

The same can be done for predecessors using the method "Graph::predecessorTopLevelDomainCounts".

Dealing with large successor or predecessor lists can be painful and viewing them in a terminal window is practically impossible. We've already discussed how to compress the list to top-level domain counts. Alternatively, you could select the labels by prefix...

```
jshell> g.successorStream("org.wikipedia", "za.org.").limit(10).forEach(System.out::println)
za.org.61mech
za.org.aadp
za.org.aag
za.org.abc
za.org.acaparty
za.org.acbio
za.org.accord
za.org.acd
za.org.acdp
za.org.acjr
```

... but even then the list may be huge. Then the best option is to write the stream output (vertex labels or top-level domain frequencies) into a file and view it later using a file viewer or use any other tool for further processing:

```
jshell> e.saveVerticesToFile(g.successors("org.wikipedia"), "org-wikipedia-successors.txt")

jshell> e.saveCountsToFile(g.successorTopLevelDomainCounts("org.wikipedia"), "org-wikipedia-successors-tld-counts.txt")
```

## Final Remarks

We hope these few examples will support either to have fun exploring the graphs or to develop your own pipeline to extract insights from the graphs.

Finally, thanks to the authors of the [WebGraph framework](https://webgraph.di.unimi.it/) and of [pyWebGraph](https://github.com/mapio/py-web-graph) for their work on these powerful tools and for any inspiration taken into these examples.
