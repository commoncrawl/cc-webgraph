# cc-webgraph

Tools to construct and process web graphs from Common Crawl data

## Compiling and Packaging Java Tools

The Java tools are compiled and packaged by [Maven](https://maven.apache.org/). If Maven is installed just run `mvn package`. Now the Java tools can be run via
```
java -cp target/cc-webgraph-0.1-SNAPSHOT-jar-with-dependencies.jar <classname> <args>...
```
The assembly jar file requires Java 10 or upwards to run. It includes also the [WebGraph](http://webgraph.di.unimi.it/) and [LAW](http://law.di.unimi.it/software.php) packages.


## Construction and Ranking of Host- and Domain-Level Web Graphs

### Host-Level Web Graph

The host-level web graph is built with help of PySpark, the corresponding code is found in the project [cc-pyspark](https://github.com/commoncrawl/cc-pyspark). Instructions are found in the script [build_hostgraph.sh](src/script/hostgraph/build_hostgraph.sh).

### Domain-Level Web Graph

The domain-level web graph is distilled from the host-level graph by mapping host names to domain names. The ID mapping is kept in memory as an int array or [FastUtil's big array](http://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/BigArrays.html) if the host-level graph has more vertices than a Java array can hold (around 2³¹). The Java tool to fold the host graph is best run from the script [host2domaingraph.sh](src/script/host2domaingraph.sh).

### Processing Graphs using the WebGraph Framework

To analyze the graph structure and calculate rankings you may further process the graphs using software from the  Laboratory for Web Algorithmics (LAW) at the University of Milano, namely the [WebGraph framework](http://webgraph.di.unimi.it/) and the [LAW library](http://law.di.unimi.it/software.php).

A couple of scripts which may help you to install the webgraph framework and run the tools to build and process the graphs are provided in [src/script/webgraph_ranking/](src/script/webgraph_ranking/). They're also used to prepare the Common Crawl web graph releases. The first script installs the webgraph and LAW software in the same directory where the scripts are located:
```
cd ./src/script/webgraph_ranking/
./install_webgraph.sh
cd ../../../
```

To process a webgraph and rank the nodes, you should first adapt the configuration to your graph and hardware setup:
```
vi ./src/script/webgraph_ranking/webgraph_config.sh
```
After running
```
./src/script/webgraph_ranking/process_webgraph.sh graph_name vertices.txt.gz edges.txt.gz output_dir
```
the `output_dir/` should contain all generated files, eg. `graph_name.graph` and `graph_name-ranks.txt.gz`.

The shell script is easily adapted to your needs. Please refer to the [LAW dataset tutorial](http://law.di.unimi.it/tutorial.php), the [API docs of LAW](http://law.di.unimi.it/software/law-docs/index.html) and [webgraph](http://webgraph.di.unimi.it/docs/) for further information.


