# cc-webgraph

Tools to construct and process web graphs from Common Crawl data

## Compiling and Packaging Java Tools

The Java tools are compiled and packaged by [Maven](http://maven.apache.org/). If it's installed just run `mvn package`.


## Construction and Ranking of Host- and Domain-Level Web Graphs

### Host-Level Web Graph

The host-level web graph is built with help of PySpark, the corresponding code is found in the project [cc-pyspark](https://github.com/commoncrawl/cc-pyspark). Instructions are found in the script [build_hostgraph.sh](src/script/hostgraph/build_hostgraph.sh).

### Domain-Level Web Graph

The domain-level web graph is distilled from the host-level graph by mapping host names to domain names. The ID mapping is kept in memory as an int array (if less than 2³¹ vertices). The Java tool to fold the host graph is best run from the script [host2domaingraph.sh](src/script/host2domaingraph.sh).