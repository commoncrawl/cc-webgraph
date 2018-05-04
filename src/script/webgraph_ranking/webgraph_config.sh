# configuration to process web graphs using the webgraph framework

# software library versions, see
#  http://webgraph.di.unimi.it/
#  http://law.di.unimi.it/software.php
WEBGRAPH_VERSION=3.6.1
WEBGRAPH_BIG_VERSION=3.5.0
LAW_VERSION=2.5

# size of the graph (default: 64 million nodes)
# - no exact size is needed, just to estimate the required Java heap space
GRAPH_SIZE_NODES=${GRAPH_SIZE_NODES:-67108864}

# for big graphs with more than 2^31 nodes/vertices
USE_WEBGRAPH_BIG=${USE_WEBGRAPH_BIG:-false}

# join node names and ranks in memory
JOIN_RANKS_IN_MEMORY=${JOIN_RANKS_IN_MEMORY:-true}


# number of threads and Hyperball registers
# depend on the size of the machine (here EC2 instance)
# ... and of the graph to be processed
# => it's only an empirical value and possibly needs to be adjusted
THREADS=2
HYP_REG=4
## on r4.16xlarge (488 GB)
THREADS=64
HYP_REG=5   # 4-6 for hostgraph, 10 for domain graph
## on x1.16xlarge (976 GB)
#THREADS=64
#HYP_REG=9
## on x1.32xlarge (1952 GB)
#THREADS=128
#HYP_REG=10

# determine automatically, using java.lang.Runtime.availableProcessors()
# THREADS=0

# number of registers used for Hyperball / harmonic centrality calculation
HYPERBALL_REGISTERS=${HYPERBALL_REGISTERS:-$HYP_REG}


# number of fields in vertices file(s)
#  (default: 2)
#   <id, name>
#  (if 3, for domain graphs)
#   <id, name, number_of_hosts_in_domain>
VERTICES_FIELDS=${VERTICES_FIELDS:-2}


# threads and buffer size used for sorting
export SORT_PARALLEL_THREADS_OPT=""
if echo -e "b\na\nc" | sort --parallel=2 >/dev/null; then
    echo "The sort command supports parallel sort threads"
    SORT_PARALLEL_THREADS_OPT="--parallel=$((($THREADS > 4) ? ($THREADS/2) : 2))"
fi

# take 10% of main memory, at least 1 GB, for sorting "chunks"
MEM_10PERC=$(free -g | perl -ne 'do { print 1+int($1*.1), "g"; last } if /(\d+)/')
export SORT_BUFFER_SIZE=${SORT_BUFFER_SIZE:-$MEM_10PERC}

# max. number of merge inputs
# (should be not less than number of vertices / edges files to be merged)
export SORT_BATCHES=${SORT_BATCHES:-160}

