#!/bin/bash

NAME="$1"
VERTICES="$2"
EDGES="$3"
if ! shift 3; then
    echo "$(basename $0) <name> <vertices> <edges> [<output_dir>]"
    exit 1
fi

OUTPUTDIR="$NAME"
if [ -n "$1" ]; then
    OUTPUTDIR="$1"
    shift
fi
FULLNAME="$OUTPUTDIR/$NAME"

if [ -d "$OUTPUTDIR" ]; then
    # TODO: OUTPUTDIR must not contain whitespace, '/', etc.
    echo "Output directory $OUTPUTDIR/ exists"
    # exit 1
else
    mkdir "$OUTPUTDIR"
fi

export LC_ALL=C

BIN=$(dirname $0)
WG=$BIN/run_webgraph.sh
LW=$BIN/run_webgraph.sh

source $BIN/../workflow_lib.sh
source $BIN/webgraph_config.sh


if ! ${USE_WEBGRAPH_BIG:-false} && [ $GRAPH_SIZE_NODES -gt $((0x7ffffff7)) ]; then
    echo "Graph has more nodes than max. array size in Java"
    echo "Using big version of webgraph framework"
    USE_WEBGRAPH_BIG=true
fi
if ${USE_WEBGRAPH_BIG:-false}; then
    WGP=it.unimi.dsi.big.webgraph
else
    WGP=it.unimi.dsi.webgraph
fi


# logging
test -d $OUTPUTDIR/logs || mkdir $OUTPUTDIR/logs
LOGDIR=$OUTPUTDIR/logs
# file to stop workflow
STOP_FILE_=$LOGDIR/$(basename $0 .sh).stop

function join_rank() (
    set -exo pipefail
    _DATA_TYPE=$1
    _IN=$2
    _VERT=$3
    _OUT=$4
    _EXTRA_FIELDS=""
    if [ -n "$5" ]; then
        _EXTRA_FIELDS=",$5"
    fi


    if [ -d $_VERT ]; then
        # _VERT is a directory with multiple vertices files
        _VERT="${_VERT}/*.gz"
    fi

    ### unpack scores with LAW, join node names via paste,
    ### assign ranks on sorted lines by nl
    $LW it.unimi.dsi.law.io.tool.DataInput2Text --type $_DATA_TYPE $_IN - \
        | paste - <(zcat $_VERT | cut -f2$_EXTRA_FIELDS) \
        | sort --batch-size=$SORT_BATCHES --buffer-size=$SORT_BUFFER_SIZE --compress-program=gzip -t$'\t' -k1,1gr --stable \
        | nl -w1 -nln \
        | gzip >$_OUT
)

function join_harmonicc_pagerank() (
    set -exo pipefail
    NAME="$1"
    _IN_HC="$2"
    _IN_PR="$3"
    _OUT="$4"
    _EXTRA_FIELDS=""
    HEADER="#harmonicc_pos\t#harmonicc_val\t#pr_pos\t#pr_val\t#host_rev"
    if [ -n "$5" ]; then
        _EXTRA_FIELDS=",$5"
        HEADER="$HEADER\t$6"
    fi
    SORTOPTS="$SORT_PARALLEL_THREADS_OPT --batch-size=$SORT_BATCHES --buffer-size=$SORT_BUFFER_SIZE --compress-program=gzip"
    (echo -e "$HEADER";
     zcat $_IN_HC | sort $SORTOPTS -t$'\t' -k3,3 --unique --stable \
         | join -a1 -a2 -e'---' -t$'\t' -j3 -o1.1,1.2,2.1,2.2,0$_EXTRA_FIELDS - \
                <(zcat $_IN_PR | sort $SORTOPTS -t$'\t' -k3,3 --unique --stable) \
         | sort $SORTOPTS -t$'\t' -k1,1n -s) \
     | gzip >$_OUT
)

function join_ranks_in_memory() (
    set -exo pipefail
    _VERT="$1"
    _HC="$2"
    _PR="$3"
    _OUT="$4"
    HEADER="#harmonicc_pos\t#harmonicc_val\t#pr_pos\t#pr_val\t#host_rev"
    if [ -n "$5" ]; then
        HEADER="${HEADER}\t$5"
    fi
    if [ -d $_VERT ]; then
        # _VERT is a directory with multiple vertices files
        _VERT="$_VERT/*.gz"
    fi
    OPTS=""
    # heuristics to set Java heap memory
    # bytes required per node (in theory, 60% more in practice)
    BYTES_MEM_REQUIRED=24
    if $USE_WEBGRAPH_BIG; then
        OPTS="--big"
        BYTES_MEM_REQUIRED=36
    fi
    BYTES_MEM_REQUIRED=$(($BYTES_MEM_REQUIRED*$GRAPH_SIZE_NODES*16/10))
    JAVA_HEAP_GB=$((($BYTES_MEM_REQUIRED/2**30)+1))
    JAVAOPTS="-Xmx${JAVA_HEAP_GB}g"
    SORTOPTS="$SORT_PARALLEL_THREADS_OPT --batch-size=$SORT_BATCHES --buffer-size=$SORT_BUFFER_SIZE --compress-program=gzip"
    (echo -e "$HEADER";
     JAVA_OPTS=$JAVA_OPTS $WG org.commoncrawl.webgraph.JoinSortRanks $OPTS <(zcat $_VERT) $_HC $_PR -) \
      | sort $SORTOPTS -t$'\t' -k1,1n --stable | gzip >$_OUT
)

function connected_distrib() (
    set -exo pipefail
    NUM_NODES=$1
    INPUT=$2
    OUTPUT=$3
    (echo -e "#freq\t#size\t#perc"; \
     $LW it.unimi.dsi.law.io.tool.DataInput2Text --type int $INPUT - \
         | perl -lne '$h{$_}++; END { while (($k,$v)=each %h) { print sprintf("%d\t%d\t%9.6f%%", $v, $k, 100*$k*$v/'$NUM_NODES') } }' \
         | sort -k2,2nr) \
        | gzip >$OUTPUT
)

function degree_distrib() (
    set -exo pipefail
    TYPE="$1"
    NAME="$2"
    (echo -e "#arcs\t#nodes";
     perl -lne 'print sprintf("%d\t%s", ($.-1), $_) if $_ ne 0' $NAME.$TYPE) \
        | gzip >$FULLNAME-$TYPE-distrib.txt.gz
)



set -exo pipefail

if [ -d $EDGES ]; then
    # edges is a directory with multiple files
    sort_input=""
    for e in $EDGES/part-*.gz; do
        sort_input="$sort_input <(zcat $e)"
    done
    if ${USE_WEBGRAPH_BIG:-false}; then
        ## TODO:
        ##    * option  --threads  not available in webgraph-big
        ##    * need to load from stdin
        ##      (fails to read longs when reading BVGraph from file)
        ##       Caused by: java.lang.IllegalArgumentException: 4635383979
        ##               at it.unimi.dsi.big.webgraph.ImmutableGraph$BigImmutableGraphAdapter.check(ImmutableGraph.java:801)
        ##               at it.unimi.dsi.big.webgraph.ImmutableGraph$BigImmutableGraphAdapter.access$200(ImmutableGraph.java:793)
        ##               at it.unimi.dsi.big.webgraph.ImmutableGraph$BigImmutableGraphAdapter$1$1.nextInt(ImmutableGraph.java:832)
        ##               at it.unimi.dsi.webgraph.LazyIntIterators.unwrap(LazyIntIterators.java:51)
        ##               at it.unimi.dsi.webgraph.NodeIterator.successorArray(NodeIterator.java:70)
        ##               at it.unimi.dsi.webgraph.ArrayListMutableGraph.<init>(ArrayListMutableGraph.java:114)
        ##               at it.unimi.dsi.big.webgraph.ArcListASCIIGraph.load(ArcListASCIIGraph.java:283)
        ##               at it.unimi.dsi.big.webgraph.ArcListASCIIGraph.load(ArcListASCIIGraph.java:279)
        ##               at it.unimi.dsi.big.webgraph.ArcListASCIIGraph.loadOffline(ArcListASCIIGraph.java:255)
        _step bvgraph \
              bash -c "eval \"sort --batch-size=$SORT_BATCHES -t$'\t' -k1,1n -k2,2n --stable --merge $sort_input\" | $WG $WGP.BVGraph --once -g $WGP.ArcListASCIIGraph - $FULLNAME"
    else
        _step bvgraph \
              bash -c "$WG $WGP.BVGraph --threads $THREADS -g $WGP.ArcListASCIIGraph <(eval \"sort --batch-size=$SORT_BATCHES -t$'\t' -k1,1n -k2,2n --stable --merge $sort_input\") $FULLNAME"
    fi
else
    if ${USE_WEBGRAPH_BIG:-false}; then
        _step bvgraph \
              bash -c "zcat $EDGES | $WG $WGP.BVGraph --once -g $WGP.ArcListASCIIGraph - $FULLNAME"
    else
        _step bvgraph \
              $WG $WGP.BVGraph --threads $THREADS -g $WGP.ArcListASCIIGraph <(zcat $EDGES) $FULLNAME
    fi
fi

if ${USE_WEBGRAPH_BIG:-false}; then
    _step transpose \
          $WG $WGP.Transform transposeOffline $FULLNAME $FULLNAME-t
else
    # if low memory, add
    # --offline, combined with
    # -Djava.io.tmpdir=... to point to a temporary directory with free space 2 times the graph size
    #     (see also run_webgraph.sh)
    _step transpose \
          $WG $WGP.Transform transpose $FULLNAME $FULLNAME-t
fi
# _step symmetrize \
#       $WG $WGP.Transform symmetrize $FULLNAME $FULLNAME-t $FULLNAME-sym

_step hyperball \
      $WG $WGP.algo.HyperBall --threads $THREADS --offline --log2m $HYPERBALL_REGISTERS \
      --harmonic-centrality $FULLNAME-harmonicc.bin $FULLNAME-t $FULLNAME

if ${USE_WEBGRAPH_BIG:-false}; then
    _step pagerank \
          $LW it.unimi.dsi.law.big.rank.PageRankParallelGaussSeidel      --mapped --threads $THREADS $FULLNAME-t $FULLNAME-pagerank
else
    _step pagerank \
          $LW it.unimi.dsi.law.rank.PageRankParallelGaussSeidel --expand --mapped --threads $THREADS $FULLNAME-t $FULLNAME-pagerank
fi

_step_bg connected 15 \
         $WG $WGP.algo.ConnectedComponents --threads $THREADS -m --renumber --sizes -t $FULLNAME-t $FULLNAME
connected_pid=$!
_step_bg strongly_connected 15 \
         $WG $WGP.algo.StronglyConnectedComponents --renumber --sizes $FULLNAME
strongly_connected_pid=$!

EXTRA_FIELDS=""
EXTRA_FIELDS_JOIN=""
EXTRA_FIELDS_HEADER=""
if [ $VERTICES_FIELDS -gt 2 ]; then
    EXTRA_FIELDS="3-$VERTICES_FIELDS"
    EXTRA_FIELDS_JOIN="1.4"
    EXTRA_FIELDS_HEADER="#n_hosts"
    for i in $(seq 4 $VERTICES_FIELDS); do
        EXTRA_FIELDS_JOIN="${EXTRA_FIELDS_JOIN},1.$(($i+1))"
    done
fi

if ${JOIN_RANKS_IN_MEMORY}; then
    _step_bg join_ranks 15 \
             join_ranks_in_memory $VERTICES $FULLNAME-harmonicc.bin $FULLNAME-pagerank.ranks $FULLNAME-ranks.txt.gz "$EXTRA_FIELDS_HEADER"
else
    _step_bg join_harmonicc 15 \
             join_rank float $FULLNAME-harmonicc.bin $VERTICES $FULLNAME-harmonic-centrality.txt.gz "$EXTRA_FIELDS"
    _step_bg join_pr_gs 15 \
             join_rank double $FULLNAME-pagerank.ranks $VERTICES $FULLNAME-pagerank.txt.gz
    wait # until background processes are finished
    # join ranks into one file
    _step_bg join_harmonicc_pagerank 60 \
             join_harmonicc_pagerank $NAME $FULLNAME-harmonic-centrality.txt.gz $FULLNAME-pagerank.txt.gz $FULLNAME-ranks.txt.gz "$EXTRA_FIELDS_JOIN" "$EXTRA_FIELDS_HEADER"
fi

# stats use connected components files, wait for these to be finished
if ! kill -0 $connected_pid; then
    : # step connected already finished
else
    wait $connected_pid
fi
if ! kill -0 $strongly_connected_pid; then
    : # step strongly_connected already finished
else
    wait $strongly_connected_pid
fi

_step stats \
         $WG $WGP.Stats --save-degrees $FULLNAME

NODES=$(perl -lne 'print if s@^nodes=@@' $FULLNAME.stats)
_step connected_distrib \
      connected_distrib $NODES $FULLNAME.wccsizes $FULLNAME-connected-components-distrib.txt.gz
# it.unimi.dsi.webgraph.Stats writes *.sccdistr (but there is no *.wccdistr)
# _step strongly_connected_distrib \
#       connected_distrib $NODES $FULLNAME.sccsizes $FULLNAME-strongly-connected-components-distrib.txt.gz

_step indegree_distrib \
      degree_distrib indegree $FULLNAME
_step outdegree_distrib \
      degree_distrib outdegree $FULLNAME

wait # until background processes are finished
