# configuration to process web graphs using the webgraph framework

# software library versions, see
#  http://webgraph.di.unimi.it/
#  http://law.di.unimi.it/software.php
WEBGRAPH_VERSION=3.6.1
WEBGRAPH_BIG_VERSION=3.5.0
LAW_VERSION=2.5

# size of the graph (default: 64 million nodes)
# - no exact size is needed, just to estimate the required Java heap space
GRAPH_SIZE_NODES=${GRAPH_SIZE:-67108864}

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
## on m2.4xlarge
#THREADS=8
#HYP_REG=8
## on m4.10xlarge
#THREADS=40
#HYP_REG=8
## on m4.16xlarge
#THREADS=64
#HYP_REG=8
## on r4.8xlarge (244 GB)
#THREADS=32
#HYP_REG=8
## on r4.16xlarge (488 GB)
THREADS=64
HYP_REG=10   # 4-8 for hostgraph, 10 for domain graph
## on x1.16xlarge (976 GB)
#THREADS=64
#HYP_REG=9
## on x1.32xlarge (1952 GB)
#THREADS=128
#HYP_REG=10

# determine automatically, using java.lang.Runtime.availableProcessors()
# THREADS=0

HYPERBALL_REGISTERS=${HYPERBALL_REGISTERS:-$HYP_REG}
