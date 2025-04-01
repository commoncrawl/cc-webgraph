# configuration to process web graphs using the webgraph framework

# size of the graph (default: 64 million nodes)
# - no exact size is needed, just to estimate the required Java heap space
GRAPH_SIZE_NODES=${GRAPH_SIZE_NODES:-67108864}

# for big graphs with more than 2^31 nodes/vertices
USE_WEBGRAPH_BIG=${USE_WEBGRAPH_BIG:-false}

# join node names and ranks in memory
JOIN_RANKS_IN_MEMORY=${JOIN_RANKS_IN_MEMORY:-true}


# number of registers used for Hyperball / harmonic centrality calculation
#
# The number of Hyperball registers depend on
# - the size of the machine (here EC2 instance)
# - and of the graph to be processed
# => it's an empirically determined value and
#    possibly needs to be adjusted
# It can be overridden by the environment variable
# HYPERBALL_REGISTERS, see below.
HYP_REG=12
## on r8.24.xlarge (768 GB, 96 CPUs)
#HYP_REG=10 (host-level graph, 300M nodes)
#HYP_REG=12 (domain-level graph, 130M nodes)

HYPERBALL_REGISTERS=${HYPERBALL_REGISTERS:-$HYP_REG}

# number of threads
# THREAD=0 : let the webgraph tools decide how many threads,
#            given the available CPU cores, using
#            java.lang.Runtime.availableProcessors()
THREADS=${THREADS:-0}



# number of fields in vertices file(s)
#  (default: 2)
#   <id, name>
#  (if 3, for domain graphs)
#   <id, name, number_of_hosts_in_domain>
VERTICES_FIELDS=${VERTICES_FIELDS:-2}


# threads and buffer size used for sorting
export SORT_PARALLEL_THREADS_OPT=""
if echo -e "b\na\nc" | sort --parallel=2 >/dev/null; then
    echo "The sort command supports parallel sort threads" >&2
    SORT_PARALLEL_THREADS_OPT="--parallel=$((($THREADS > 4) ? ($THREADS/2) : 2))"
fi

# take 20% of main memory, at least 1 GB, for sorting "chunks"
MEM_20PERC=$(free -g | perl -ne 'do { print 1+int($1*.2), "g"; last } if /(\d+)/')
export SORT_BUFFER_SIZE=${SORT_BUFFER_SIZE:-$MEM_20PERC}

# max. number of merge inputs
# (should be not less than number of vertices / edges files to be merged)
export SORT_BATCHES=${SORT_BATCHES:-240}

