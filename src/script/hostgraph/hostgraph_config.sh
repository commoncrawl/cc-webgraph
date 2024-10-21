################################################################################
### configuration of Common Crawl webgraph releases
### (sourced from other scripts)
################################################################################


################################################################################
### Exctraction of inter-host links from
###  - WAT files and
###  - non-200 responses WARC files for redirects
###  - (optionally) sitemap directives in robots.txt files
### saved as tuples <from_host, to_host>

# crawls to be processed
CRAWLS=("CC-MAIN-2024-33" "CC-MAIN-2024-38" "CC-MAIN-2024-42")

INPUT_BASE_URL="s3://commoncrawl/"

# whether to include links to sitemaps contained in robots.txt files
# Note: often links to sitemap indicate relations between domain owners.
INCLUDE_ROBOTSTXT_SITEMAP_LINKS=true

# whether to construct a host-level graph for each input crawl
CONSTRUCT_HOSTGRAPH=false

# max. number of input files (WARC/WAT) per Spark job
# - splits hostlink extraction into multiple jobs
# - output is checkpointed on S3 after each job
#   (useful if cluster runs on spot instances)
MAX_INPUT_SIZE=64000

# hdfs:// directory where input and output is kept
HDFS_BASE_DIR=hdfs:///user/ubuntu/webgraph
WAREHOUSE_DIR=$HDFS_BASE_DIR

# where to keep results on s3://
# (note: this is a private bucket and needs to be changed)
S3_OUTPUT_PREFIX=s3://commoncrawl-webgraph
S3A_OUTPUT_PREFIX=s3a://commoncrawl-webgraph


################################################################################
# construct a merged graph of multiple monthly crawls

MERGE_NAME=cc-main-2024-aug-sep-oct

# Naming convention should be the three months' crawls that are
# used to generate this graph release. In the event of multiple months
# in a crawl, (e.g August & September, November & December) the first month is
# used (e.g aug-nov).

# input to construct a merged graph (over multiple months)
# - used in addition to input crawls (see CRAWLS)
# - output directories of hostlinks jobs of prior crawls
# - list of fully-qualified paths:
#   ("s3a://.../hostlinks/0/" "s3a://.../hostlinks/1/" ...)
# - ev. copy the data from s3:// to hdfs:// to avoid tasks
#   taking long while reading from S3
MERGE_INPUT=()


################################################################################
# workflow runtime

# temporary directory
# - must exist on task/compute nodes for buffering data
# - should provide several GBs of free space
TMPDIR=/data/0/tmp

# where to keep logs for steps
LOGDIR=$PWD

# file to stop the workflow (stops after a the currently running step(s) are done)
STOP_FILE_=$LOGDIR/$(basename "$0" .sh).stop

# use Python executable different than default "python"
export PYSPARK_PYTHON=python3

################################################################################


################################################################################
### Spark / Yarn cluster configuration
NUM_EXECUTORS=${NUM_EXECUTORS:-16}
EXECUTOR_CONFIG=${EXECUTOR_CONFIG:-"r5.xlarge"}
# NOTE:
#  - step 1 (host link extraction) can be run on smaller instances
#    or "compute optimized" instance types
#  - webgraph construction (esp. for merged graphs including multiple monthyl crawls)
#    needs instances with sufficient amount of RAM (32 GB or more)
#  - assigning IDs in multiple partitions
#    (see hostlinks_to_graph.py --vertex_partitions)
#    reduces the memory requirements significantly


case "$EXECUTOR_CONFIG" in
    c[34567]*.xlarge )
        EXECUTOR_CORES=3
        EXECUTOR_MEM=5g
        NODEMANAGER_MEM_MB=$((6*1024))
        ;;
    c[34567]*.2xlarge )
        EXECUTOR_CORES=6
        EXECUTOR_MEM=10g
        NODEMANAGER_MEM_MB=$((11*1024))
        ;;
    c[34567]*.4xlarge )
        EXECUTOR_CORES=12
        EXECUTOR_MEM=22g
        NODEMANAGER_MEM_MB=$((24*1024))
        ;;
    r[34567]*.xlarge )
        EXECUTOR_CORES=3
        EXECUTOR_MEM=23g
        NODEMANAGER_MEM_MB=$((24*1024))
        ;;
    r[34567]*.2xlarge )
        EXECUTOR_CORES=6
        EXECUTOR_MEM=46g
        NODEMANAGER_MEM_MB=$((48*1024))
        ;;
    r[34567]*.4xlarge )
        EXECUTOR_CORES=12
        EXECUTOR_MEM=94g
        NODEMANAGER_MEM_MB=$((96*1024))
        ;;
    r[34567]*.8xlarge )
        EXECUTOR_CORES=24
        EXECUTOR_MEM=190g
        NODEMANAGER_MEM_MB=$((192*1024))
        ;;
    m[34567]*.2xlarge )
        EXECUTOR_CORES=8
        EXECUTOR_MEM=23g
        NODEMANAGER_MEM_MB=$((24*1024))
        ;;
    m[34567]*.4xlarge )
        EXECUTOR_CORES=16
        EXECUTOR_MEM=46g
        NODEMANAGER_MEM_MB=$((48*1024))
        ;;
    m[34567]*.8xlarge )
        EXECUTOR_CORES=32
        EXECUTOR_MEM=94g
        NODEMANAGER_MEM_MB=$((98*1024))
        ;;
    "custom" )
        if [ -z "$EXECUTOR_CORES" ] || [ -z "$EXECUTOR_MEM" ]; then
            echo "No valid custom executor configuration: must specify EXECUTOR_CORES and EXECUTOR_MEM'" >&2
            exit 1
        fi
        ;;
    * )
        echo "No valid executor configuration: '$EXECUTOR_CONFIG'" >&2
        exit 1
esac

SPARK_EXTRA_OPTS="$SPARK_EXTRA_OPTS --conf spark.yarn.nodemanager.resource.memory-mb=$NODEMANAGER_MEM_MB"

OUTPUT_PARTITIONS=$((NUM_EXECUTORS*EXECUTOR_CORES/2))
WEBGRAPH_EDGE_PARTITIONS=$((NUM_EXECUTORS*EXECUTOR_CORES/2))
WEBGRAPH_EDGE_PARTITIONS=$(((WEBGRAPH_EDGE_PARTITIONS<NUM_EXECUTORS)?NUM_EXECUTORS:WEBGRAPH_EDGE_PARTITIONS))
WEBGRAPH_VERTEX_PARTITIONS=$((NUM_EXECUTORS*EXECUTOR_CORES/4))
WEBGRAPH_VERTEX_PARTITIONS=$(((WEBGRAPH_VERTEX_PARTITIONS<NUM_EXECUTORS)?NUM_EXECUTORS:WEBGRAPH_VERTEX_PARTITIONS))
DIVISOR_INPUT_PARTITIONS=5
