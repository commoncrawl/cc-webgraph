# Interactive Graph Exploration

A tutorial how to interactively explore the Common Crawl webgraphs – or other graphs using the webgraph format – using the [JShell](https://docs.oracle.com/en/java/javase/21/jshell/index.html) and the [GraphExplorer](src/main/java/org/commoncrawl/webgraph/explore/GraphExplorer.java) class.


## Quick Start

1. change into the "cc-webgraph" project directory, [build the cc-webgraph jar](README.md#compiling-and-packaging-java-tools) and remember the project directory using an environment variable

   ```
   $> cd .../cc-webgraph

   $> mvn clean package

   $> CC_WEBGRAPH=$PWD
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
   $> jshell --class-path $CC_WEBGRAPH/target/cc-webgraph-*-jar-with-dependencies.jar
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
   $> jshell --class-path $CC_WEBGRAPH/target/cc-webgraph-*-jar-with-dependencies.jar \
             -R-Dgraph=$GRAPH \
             $CC_WEBRAPH/src/script/webgraph_ranking/graph_explore_load_graph.jsh
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

