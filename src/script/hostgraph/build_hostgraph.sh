#!/bin/bash

# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2022 Common Crawl and contributors

set -e
set -o pipefail
set -x

# run the webgraph workflow (based on cc-pyspark)
#  - extract inter-host links
#  - construct the host-level graph

# installation and execution:
#   - install cc-pyspark
#      git clone https://github.com/commoncrawl/cc-pyspark.git
#   - and make it the working directory
#      cd cc-pyspark
#   - point SPARK_HOME to your installation of Apache Spark (https://spark.apache.org/)
#      vi ./spark_env.sh
#     and make sure that your Spark cluster (on Hadoop YARN) is running!
#   - edit the hostgraph build configuration
#      vi .../hostgraph_config.sh
#   - run the workflow
#      .../build_hostgraph.sh

# Note: the script is tested using a Hadoop cluster running
# Apache Bigtop 3.x on Ubuntu 22.04. You may need to adapt it
# to run on different Hadoop distributions.


SPARK_ON_YARN="--master yarn"
SPARK_HADOOP_OPTS=""
SPARK_EXTRA_OPTS=""

HOST_LINK_EXTRACTOR=./hostlinks_extract_fastwarc.py
PYFILES_HOST_LINK_EXTRACTOR="sparkcc.py,sparkcc_fastwarc.py,wat_extract_links.py"

HOST_LINKS_TO_GRAPH=./hostlinks_to_graph.py
PYFILES_HOST_LINKS_TO_GRAPH="sparkcc.py,iana_tld.py,wat_extract_links.py"
HOST_LINKS_TO_GRAPH_ARGS=(--validate_host_names) # --normalize_host_names


# source library functions
source "$(dirname "$0")"/../workflow_lib.sh

# source workflow configuration
source "$(dirname "$0")"/hostgraph_config.sh

# define SPARK_HOME and HADOOP_CONF_DIR
source "$PWD"/spark_env.sh


################################################################################

# upload Parquet graph
function upload_parquet() (
    set -xeo pipefail
    TABLE=$1
    UPLOAD_NAME=$2
    UPLOAD_DIR=$S3A_OUTPUT_PREFIX/$UPLOAD_NAME/hostgraph
    if hadoop fs -test -d "$UPLOAD_DIR"/vertices; then
        echo "Upload $UPLOAD_DIR/vertices already exists, skipping..."
    else
        hadoop distcp \
               "$HDFS_BASE_DIR"/${TABLE}_vertices \
               "$UPLOAD_DIR"/vertices
    fi
    if hadoop fs -test -d "$UPLOAD_DIR"/edges; then
        echo "Upload "$UPLOAD_DIR"/edges already exists, skipping..."
    else
        hadoop distcp \
               "$HDFS_BASE_DIR"/${TABLE}_edges \
               "$UPLOAD_DIR"/edges
    fi
)

function upload_text() (
    set -xeo pipefail
    NAME=$1
    UPLOAD_NAME=$2
    UPLOAD_DIR="$S3A_OUTPUT_PREFIX"/$UPLOAD_NAME/hostgraph/text
    PUBLIC=${3:-false}
    DISTCP_OPTS=""
    if $PUBLIC; then
        DISTCP_OPTS="$DISTCP_OPTS -Dfs.s3a.acl.default=PublicRead"
    fi
    if hadoop fs -test -d "$UPLOAD_DIR"/vertices; then
        echo "Upload $UPLOAD_DIR/vertices already exists, skipping..."
    else
        hadoop fs -rm -f "$HDFS_BASE_DIR"/text/$NAME/vertices/_SUCCESS
        hadoop distcp $DISTCP_OPTS \
               "$HDFS_BASE_DIR"/text/$NAME/vertices \
               "$UPLOAD_DIR"/vertices
    fi
    if hadoop fs -test -d "$UPLOAD_DIR"/edges; then
        echo "Upload $UPLOAD_DIR/edges already exists, skipping..."
    else
        hadoop fs -rm -f "$HDFS_BASE_DIR"/text/$NAME/edges/_SUCCESS
        hadoop distcp $DISTCP_OPTS \
               "$HDFS_BASE_DIR"/text/$NAME/edges \
               "$UPLOAD_DIR"/edges
    fi
)

# text output
function dump_upload_text() (
    set -xeo pipefail
    NAME=$1
    UPLOAD_NAME=$2
    mkdir -p output/$NAME/hostgraph/tmp_edges/
    mkdir -p output/$NAME/hostgraph/tmp_vertices/
    hadoop fs -copyToLocal "$HDFS_BASE_DIR"/text/$NAME/vertices/*.gz output/$NAME/hostgraph/tmp_vertices/
    n_vertex_files=$(ls output/$NAME/hostgraph/tmp_vertices/*.gz | wc -l)
    if [ $n_vertex_files -eq 1 ]; then
        mv output/$NAME/hostgraph/tmp_vertices/*.gz output/$NAME/hostgraph/vertices.txt.gz
    else
        zcat output/$NAME/hostgraph/tmp_vertices/*.gz | gzip >output/$NAME/hostgraph/vertices.txt.gz
    fi
    aws s3 cp --no-progress output/$NAME/hostgraph/vertices.txt.gz $S3_OUTPUT_PREFIX/$UPLOAD_NAME/hostgraph/
    hadoop fs -copyToLocal "$HDFS_BASE_DIR"/text/$NAME/edges/*.gz output/$NAME/hostgraph/tmp_edges/
    sort_input=""
    for e in output/$NAME/hostgraph/tmp_edges/*.gz; do
        sort_input="$sort_input <(zcat $e)"
    done
    mkdir -p tmp
    eval "sort --batch-size 96 --buffer-size 4g --parallel 2 --temporary-directory ./tmp/ --compress-program=gzip -t$'\t' -k1,1n -k2,2n --stable --merge $sort_input | gzip >output/$NAME/hostgraph/edges.txt.gz"
    aws s3 cp --no-progress output/$NAME/hostgraph/edges.txt.gz    $S3_OUTPUT_PREFIX/$UPLOAD_NAME/hostgraph/
)

function create_input_splits() {
    CRAWL="$1"
    __INPUT_SPLITS=()
    if [ -d input/$CRAWL/ ]; then
        # input splits are already created locally, read the splits again
        # (this might happen if one of the steps/jobs has failed and
        #  this script is run again)
        for split in input/$CRAWL/input_split_*.txt; do
            __INPUT_SPLITS=(${__INPUT_SPLITS[@]} "$HDFS_BASE_DIR/$split")
        done

    elif hadoop fs -stat "$S3A_OUTPUT_PREFIX"/$CRAWL/hostlinks/ >&2; then
        # no local input splits but output on S3
        echo "Not creating input split for crawl $CRAWL because output prefix already exists on S3: $S3A_OUTPUT_PREFIX/$CRAWL/hostlinks/" >&2

    else
        mkdir -p input/$CRAWL
        cd input/$CRAWL
        aws s3 cp --quiet --no-progress s3://commoncrawl/crawl-data/$CRAWL/wat.paths.gz .
        aws s3 cp --quiet --no-progress s3://commoncrawl/crawl-data/$CRAWL/non200responses.paths.gz .
        if $INCLUDE_ROBOTSTXT_SITEMAP_LINKS; then
            aws s3 cp --quiet --no-progress s3://commoncrawl/crawl-data/$CRAWL/robotstxt.paths.gz .
        fi
        zcat ./*.paths.gz | shuf >input.txt
        NUM_INPUT_PATHS=$(wc -l <input.txt)
        NUM_SPLITS=$((1+NUM_INPUT_PATHS/MAX_INPUT_SIZE))
        if [ $NUM_SPLITS -gt 0 ]; then
            split --suffix-length=2 -d --lines=$((1+NUM_INPUT_PATHS/NUM_SPLITS)) input.txt input_split_
        fi
        for split in input_split_*; do
            mv $split $split.txt
            __INPUT_SPLITS=(${__INPUT_SPLITS[@]} "$HDFS_BASE_DIR/input/$CRAWL/$split.txt")
        done
        cd - >&2
        ### copy input to hdfs://
        hadoop fs -mkdir -p "$HDFS_BASE_DIR"/$CRAWL
        hadoop fs -mkdir -p "$HDFS_BASE_DIR"/input/$CRAWL/
        hadoop fs -mkdir -p "$HDFS_BASE_DIR"/text/$CRAWL/
        hadoop fs -copyFromLocal -f input/$CRAWL/input.txt "$HDFS_BASE_DIR"/input/$CRAWL/
        for split in input/$CRAWL/input_split_*.txt; do
            hadoop fs -copyFromLocal -f $split "$HDFS_BASE_DIR"/input/$CRAWL/
        done
        # The input list is considerably small because it only references s3:// paths:
        # deploy it on every node to make all tasks NODE_LOCAL
        hadoop fs -setrep $((NUM_EXECUTORS+1)) "$HDFS_BASE_DIR"/input/$CRAWL/ >&2
    fi
    echo "${__INPUT_SPLITS[@]}"
}


################################################################################

MERGE_CRAWL_INPUT=""

for CRAWL in ${CRAWLS[@]}; do

    INPUT_SPLITS=($(create_input_splits $CRAWL))

    if [ -z "$INPUT_SPLITS" ]; then
        # no input splits signals that the crawl has already successfully processed
        if hadoop fs -stat "$S3A_OUTPUT_PREFIX"/$CRAWL/hostlinks/; then
            echo "Output prefix for crawl $CRAWL already exists on S3: $S3A_OUTPUT_PREFIX/$CRAWL/hostlinks/"
            if ! hadoop fs -stat "$S3A_OUTPUT_PREFIX"/$CRAWL/hostlinks/_SUCCESS; then
                echo "No success marker found below S3 output prefix: $S3A_OUTPUT_PREFIX/$CRAWL/hostlinks/_SUCCESS"
                echo "Please, verify the output and depending on the verification result, manually add the success marker or remove the output. Exiting ..."
                exit 1
            fi
        fi
        # add the existing output splits as input for host graph and merged graph
        for output_split in $(hadoop fs -ls -C "$S3A_OUTPUT_PREFIX"/$CRAWL/hostlinks/); do
            if [ -z "$HOSTGRAPH_INPUT" ]; then
                HOSTGRAPH_INPUT="$output_split"
            else
                HOSTGRAPH_INPUT="--add_input $output_split $HOSTGRAPH_INPUT"
            fi
            if [ -z "$MERGE_CRAWL_INPUT" ]; then
                MERGE_CRAWL_INPUT="$output_split"
            else
                MERGE_CRAWL_INPUT="--add_input $output_split $MERGE_CRAWL_INPUT"
            fi
        done

    else
        echo "Input splits: ""${INPUT_SPLITS[*]}"

        for ((i=0; i<${#INPUT_SPLITS[@]}; i++)); do
            INPUT=${INPUT_SPLITS[$i]}
            NUM_INPUT_PATHS=$(wc -l <input/$CRAWL/$(basename $INPUT))
            INPUT_PARTITIONS=$((NUM_INPUT_PATHS/DIVISOR_INPUT_PARTITIONS))
            echo "$INPUT => $INPUT_PARTITIONS partitions"

            _step hostlinks.$CRAWL.split$i \
                  "$SPARK_HOME"/bin/spark-submit \
                  $SPARK_ON_YARN \
                  $SPARK_HADOOP_OPTS \
                  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                  --conf spark.task.maxFailures=80 \
                  --conf spark.executor.memory=$EXECUTOR_MEM \
                  --conf spark.driver.memory=6g \
                  --conf spark.core.connection.ack.wait.timeout=600s \
                  --conf spark.network.timeout=300s \
                  --conf spark.shuffle.io.maxRetries=5 \
                  --conf spark.shuffle.io.retryWait=30s \
                  --conf spark.io.compression.codec=zstd \
                  --conf spark.checkpoint.compress=true \
                  --conf spark.locality.wait=0s \
                  --num-executors $NUM_EXECUTORS \
                  --executor-cores $EXECUTOR_CORES \
                  --executor-memory $EXECUTOR_MEM \
                  --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR/$CRAWL \
                  --conf spark.sql.parquet.compression.codec=zstd \
                  --py-files "$PYFILES_HOST_LINK_EXTRACTOR" \
                  $SPARK_EXTRA_OPTS \
                  $HOST_LINK_EXTRACTOR \
                  --input_base_url $INPUT_BASE_URL \
                  --num_input_partitions $INPUT_PARTITIONS \
                  --num_output_partitions $OUTPUT_PARTITIONS \
                  --local_temp_dir "$TMPDIR" \
                  $INPUT hostlinks$i

            _step hostlinks.$CRAWL.split$i.distcp \
                  hadoop distcp \
                  -Dfs.s3a.connection.timeout=2000 \
                  -Dfs.s3a.attempts.maximum=3 \
                  "$HDFS_BASE_DIR"/$CRAWL/hostlinks$i \
                  "$S3A_OUTPUT_PREFIX"/$CRAWL/hostlinks/$i

            if [ -z "$HOSTGRAPH_INPUT" ]; then
                HOSTGRAPH_INPUT="$HDFS_BASE_DIR/$CRAWL/hostlinks$i"
            else
                HOSTGRAPH_INPUT="--add_input $HDFS_BASE_DIR/$CRAWL/hostlinks$i $HOSTGRAPH_INPUT"
            fi
            if [ -z "$MERGE_CRAWL_INPUT" ]; then
                MERGE_CRAWL_INPUT="$HDFS_BASE_DIR/$CRAWL/hostlinks$i"
            else
                MERGE_CRAWL_INPUT="--add_input $HDFS_BASE_DIR/$CRAWL/hostlinks$i $MERGE_CRAWL_INPUT"
            fi
        done # end input splits

        # Create the success marker on S3
        hadoop fs -touchz "$S3A_OUTPUT_PREFIX"/$CRAWL/hostlinks/_SUCCESS

    fi


    if $CONSTRUCT_HOSTGRAPH; then

        if hadoop fs -stat "$S3A_OUTPUT_PREFIX"/$CRAWL/hostgraph/; then
            echo "Skipping creation of hostgraph for crawl $CRAWL because output prefix already exists on S3: $S3A_OUTPUT_PREFIX/$CRAWL/hostgraph/"
            continue
        fi

        VERTEX_IDS=""
        if hadoop fs -stat "$HDFS_BASE_DIR"/$CRAWL/hostgraph_vertices; then
            VERTEX_IDS="--vertex_ids $HDFS_BASE_DIR/$CRAWL/hostgraph_vertices"
        fi

        _step hostgraph.$CRAWL \
              "$SPARK_HOME"/bin/spark-submit \
              $SPARK_ON_YARN \
              $SPARK_HADOOP_OPTS \
              --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
              --conf spark.task.maxFailures=10 \
              --conf spark.executor.memory=$EXECUTOR_MEM \
              --conf spark.driver.memory=6g \
              --conf spark.core.connection.ack.wait.timeout=600s \
              --conf spark.network.timeout=300s \
              --conf spark.shuffle.io.maxRetries=5 \
              --conf spark.shuffle.io.retryWait=30s \
              --conf spark.locality.wait=1s \
              --conf spark.io.compression.codec=zstd \
              --conf spark.checkpoint.compress=true \
              --num-executors $NUM_EXECUTORS \
              --executor-cores $EXECUTOR_CORES \
              --executor-memory $EXECUTOR_MEM \
              --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR/$CRAWL \
              --conf spark.sql.parquet.compression.codec=zstd \
              --py-files "$PYFILES_HOST_LINKS_TO_GRAPH" \
              $SPARK_EXTRA_OPTS \
              $HOST_LINKS_TO_GRAPH \
              "${HOST_LINKS_TO_GRAPH_ARGS[@]}" \
              --save_as_text "$HDFS_BASE_DIR"/text/$CRAWL \
              --num_output_partitions $WEBGRAPH_EDGE_PARTITIONS \
              --local_temp_dir $TMPDIR \
              $VERTEX_IDS \
              $HOSTGRAPH_INPUT hostgraph


        _step hostgraph.$CRAWL.upload.1 \
              upload_parquet hostgraph $CRAWL

        _step hostgraph.$CRAWL.upload.2 \
              dump_upload_text $CRAWL $CRAWL
    fi

done # CRAWLS



if [ -n "MERGE_NAME" ]; then

    hadoop fs -mkdir -p "$HDFS_BASE_DIR"/text/$MERGE_NAME

    for INP in "${MERGE_INPUT[@]}"; do
        if [ -z "$MERGE_CRAWL_INPUT" ]; then
            MERGE_CRAWL_INPUT="$INP"
        else
            MERGE_CRAWL_INPUT="--add_input $INP $MERGE_CRAWL_INPUT"
        fi
    done

    VERTEX_IDS=""
    if hadoop fs -test -d "$HDFS_BASE_DIR"/hostgraph_merged_vertices; then
        VERTEX_IDS="--vertex_ids $HDFS_BASE_DIR/hostgraph_merged_vertices"
    fi

    _step hostgraph_merged \
      "$SPARK_HOME"/bin/spark-submit \
        $SPARK_ON_YARN \
        $SPARK_HADOOP_OPTS \
        --py-files "$PYFILES_HOST_LINKS_TO_GRAPH" \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.task.maxFailures=10 \
        --conf spark.executor.memory=$EXECUTOR_MEM \
        --conf spark.driver.memory=6g \
        --conf spark.core.connection.ack.wait.timeout=600s \
        --conf spark.network.timeout=300s \
        --conf spark.shuffle.io.maxRetries=5 \
        --conf spark.shuffle.io.retryWait=30s \
        --conf spark.locality.wait=1s \
        --conf spark.io.compression.codec=zstd \
        --conf spark.checkpoint.compress=true \
        --num-executors $NUM_EXECUTORS \
        --executor-cores $EXECUTOR_CORES \
        --executor-memory $EXECUTOR_MEM \
        --conf spark.sql.warehouse.dir=$WAREHOUSE_DIR \
        --conf spark.sql.parquet.compression.codec=zstd \
        $SPARK_EXTRA_OPTS \
        $HOST_LINKS_TO_GRAPH \
        "${HOST_LINKS_TO_GRAPH_ARGS[@]}" \
        --save_as_text "$HDFS_BASE_DIR"/text/$MERGE_NAME \
        --vertex_partitions $WEBGRAPH_VERTEX_PARTITIONS \
        --num_output_partitions $WEBGRAPH_EDGE_PARTITIONS \
        --local_temp_dir "$TMPDIR" \
        $VERTEX_IDS \
        $MERGE_CRAWL_INPUT hostgraph_merged

    _step hostgraph_merged.upload.1 \
          upload_parquet hostgraph_merged $MERGE_NAME

    _step hostgraph_merged.upload.2 \
          upload_text $MERGE_NAME $MERGE_NAME true

    ### merge (one file for vertices, one for edges) and upload
    # _step hostgraph_merged.upload.2 \
    #       dump_upload_text $MERGE_NAME $MERGE_NAME

elif [ -n "$MERGE_INPUT" ]; then

    echo "MERGE_INPUT is defined, but no MERGE_NAME given?"
    exit 1

fi
