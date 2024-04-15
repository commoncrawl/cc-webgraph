# cc-webgraph

Tools to construct and process web graphs from Common Crawl data

## Compiling and Packaging Java Tools

Java 11 or upwards are required.

The Java tools are compiled and packaged by [Maven](https://maven.apache.org/). If Maven is installed just run `mvn package`. Now the Java tools can be run via
```
java -cp target/cc-webgraph-0.1-SNAPSHOT-jar-with-dependencies.jar <classname> <args>...
```

The assembly jar file includes also the [WebGraph](https://webgraph.di.unimi.it/) and [LAW](https://law.di.unimi.it/software.php) packages required to compute [PageRank](https://en.wikipedia.org/wiki/PageRank) and [Harmonic Centrality](https://en.wikipedia.org/wiki/Centrality#Harmonic_centrality).


### Javadocs

The Javadocs are created by `mvn javadoc:javadoc`. Then open the file `target/site/apidocs/index.html` in a browser.


## Memory and Disk Requirements

Note that the webgraphs are usually multiple Gigabytes in size and require for processing
- a sufficient Java heap size ([Java option](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html#extra-options-for-java) `-Xmx`)
- enough disk space to store the graphs and temporary data.

The exact requirements depend on the graph size and the task – graph exploration or ranking, etc.


## Construction and Ranking of Host- and Domain-Level Web Graphs

### Host-Level Web Graph

The host-level web graph is built with help of PySpark, the corresponding code is found in the project [cc-pyspark](https://github.com/commoncrawl/cc-pyspark). Instructions are found in the script [build_hostgraph.sh](src/script/hostgraph/build_hostgraph.sh).

### Domain-Level Web Graph

The domain-level web graph is distilled from the host-level graph by mapping host names to domain names. The ID mapping is kept in memory as an int array or [FastUtil's big array](https://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/BigArrays.html) if the host-level graph has more vertices than a Java array can hold (around 2³¹). The Java tool to fold the host graph is best run from the script [host2domaingraph.sh](src/script/host2domaingraph.sh).

### Processing Graphs using the WebGraph Framework

To analyze the graph structure and calculate rankings you may further process the graphs using software from the Laboratory for Web Algorithmics (LAW) at the University of Milano, namely the [WebGraph framework](https://webgraph.di.unimi.it/) and the [LAW library](https://law.di.unimi.it/software.php).

A couple of scripts may help you to run the WebGraph tools to build and process the graphs are provided in [src/script/webgraph_ranking/](src/script/webgraph_ranking/). They're also used to prepare the Common Crawl web graph releases.

To process a webgraph and rank the nodes, you should first adapt the configuration to your graph and hardware setup:
```
vi ./src/script/webgraph_ranking/webgraph_config.sh
```
After running
```
./src/script/webgraph_ranking/process_webgraph.sh graph_name vertices.txt.gz edges.txt.gz output_dir
```
the `output_dir/` should contain all generated files, eg. `graph_name.graph` and `graph_name-ranks.txt.gz`.

The shell script is easily adapted to your needs. Please refer to the [LAW dataset tutorial](https://law.di.unimi.it/tutorial.php), the [API docs of LAW](https://law.di.unimi.it/software/law-docs/index.html) and [webgraph](https://webgraph.di.unimi.it/docs/) for further information.


## Exploring Webgraph Data Sets

The Common Crawl webgraph data sets are announced on the [Common Crawl web site](https://commoncrawl.org/tag/webgraph/).

For instructions how to explore the webgraphs using the JShell please see the tutorial [Interactive Graph Exploration](./graph-exploration-README.md). For an older approach using [Jython](https://www.jython.org/) and [pyWebGraph](https://github.com/mapio/py-web-graph), see the [cc-notebooks project](//github.com/commoncrawl/cc-notebooks/tree/master/cc-webgraph-statistics).


## Credits

Thanks to the authors of the [WebGraph framework](https://webgraph.di.unimi.it/) used to process the graphs and compute page rank and harmonic centrality. See also Sebastiano Vigna's projects [webgraph](//github.com/vigna/webgraph) and [webgraph-big](//github.com/vigna/webgraph-big).
