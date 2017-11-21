################################################################################
### configuration of Common Crawl webgraph releases
### (sourced from other scripts)
################################################################################


################################################################################
### Exctraction of inter-host links from
###  - WAT files and
###  - non-200 responses WARC files for redirects
### saved as tuples <from_host, to_host>

# crawls to be processed
CRAWLS=("CC-MAIN-2017-34" "CC-MAIN-2017-39" "CC-MAIN-2017-43")

# whether to construct a host-level graph for each input crawl
CONSTRUCT_HOSTGRAPH=false

# max. number of input files (WARC/WAT) per Spark job
# - splits hostlink extraction into multiple jobs
# - output is checkpointed on S3 after each job
#   (useful if cluster runs on spot instances)
MAX_INPUT_SIZE=40000

# hdfs:// directory where input and output is kept
HDFS_BASE_DIR=hdfs:///user/ubuntu/webgraph
WAREHOUSE_DIR=$HDFS_BASE_DIR

# where to keep results on s3://
S3_OUTPUT_PREFIX=s3://my-webgraph
S3A_OUTPUT_PREFIX=s3a://my-webgraph


################################################################################
# construct a merged graph of multiple monthly crawls

MERGE_NAME=cc-main-2017-aug-sep-oct

# input to construct a merged graph (over multiple months)
# - used in addition to input crawls (see CRAWLS)
# - output directories of hostlinks jobs of prior crawls

MERGE_INPUT=()  # ("s3a://.../hostlinks/0/" "s3a://.../hostlinks/1/" ...)
# NOTE: ev. copy the data from s3:// to hdfs:// to avoid tasks
#       taking long while reading from S3


################################################################################
# workflow runtime

# temporary directory
# - must exist on task/compute nodes for buffering data
# - should provide several GBs of free space
TMPDIR=/data/1/tmp

# where to keep logs for steps
LOGDIR=$PWD

# file to stop the workflow (stops after a the currently running step(s) are done)
STOP_FILE_=$LOGDIR/$(basename $0 .sh).stop

################################################################################


################################################################################
### Spark / Yarn cluster configuration
NUM_EXECUTORS=6
EXECUTOR_CONFIG="r3.8xlarge"
# NOTE:
#  - step 1 (host link extraction) can be run on smaller instances
#  - webgraph construction (esp. for merged graphs) needs instances
#    with sufficient amount of RAM


case "$EXECUTOR_CONFIG" in
    "m2.xlarge" )
        EXECUTOR_CORES=4
        EXECUTOR_MEM=6g
        ;;
    "m2.4xlarge" )
        EXECUTOR_CORES=12
        EXECUTOR_MEM=40g
        ;;
    "c3.xlarge" )
        EXECUTOR_CORES=6
        EXECUTOR_MEM=5g
        ;;
    "r3.xlarge" )
        EXECUTOR_CORES=6
        EXECUTOR_MEM=24g
        ;;
    "r3.2xlarge" )
        EXECUTOR_CORES=12
        EXECUTOR_MEM=48g
        ;;
    "r3.4xlarge" )
        EXECUTOR_CORES=24
        EXECUTOR_MEM=96g
        ;;
    "r3.8xlarge" )
        EXECUTOR_CORES=48
        EXECUTOR_MEM=192g
        ;;
    * )
        echo "No valid executor configuration: '$EXECUTOR_CONFIG'" >&2
        exit 1
esac

OUTPUT_PARTITIONS=$(($NUM_EXECUTORS*$EXECUTOR_CORES/2))
WEBGRAPH_EDGE_PARTITIONS=$(($NUM_EXECUTORS*$EXECUTOR_CORES/4))
WEBGRAPH_VERTEX_PARTITIONS=$(($NUM_EXECUTORS*$EXECUTOR_CORES/4))
DIVISOR_INPUT_PARTITIONS=5
