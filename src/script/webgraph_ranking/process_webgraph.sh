#!/bin/bash

NAME="$1"
VERTICES="$2"
EDGES="$3"
if ! shift 3; then
    echo "$(basename $0) <name> <vertices> <edges>"
    exit 1
fi

if [ -d "$NAME" ]; then
	# TODO: NAME must not contain whitespace, '/', etc.
    echo "Output directory $NAME/ exists"
    # exit 1
else
    mkdir "$NAME"
fi

export LC_ALL=C

BIN=$(dirname $0)
WG=$BIN/run_webgraph.sh
WB=$BIN/run_webgraph_big.sh
LW=$BIN/run_law.sh

source $BIN/../workflow_lib.sh
source $BIN/webgraph_config.sh


if ${USE_WEBGRAPH_BIG:-false}; then
    WG=$WB
    WGP=it.unimi.dsi.big.webgraph
else
    WGP=it.unimi.dsi.webgraph
fi

# logging
test -d $NAME/logs || mkdir $NAME/logs
LOGDIR=$NAME/logs
# file to stop workflow
STOP_FILE_=$LOGDIR/$(basename $0 .sh).stop


function unpack_float() {
    perl -e '$n = 0;
               while (0 != read(STDIN, $block, 65536)) {
                 for (unpack("f>*", $block)) {
                   print $n, "\t", $_, "\n"; $n++;
                 }
               }'
}

function unpack_double() {
    perl -e '$n = 0;
               while (0 != read(STDIN, $block, 65536)) {
                 for (unpack("d>*", $block)) {
                   print $n, "\t", $_, "\n"; $n++;
                 }
               }'
}

function unpack_int() {
    perl -e '$n = 0;
               while (0 != read(STDIN, $block, 65536)) {
                 for (unpack("l>*", $block)) {
                   print $n, "\t", $_, "\n"; $n++;
                 }
               }'
}

function unpack_file() {
    _FUNC=$1
    _IN=$2
    _OUT=$3
    $_FUNC <$_IN | gzip >$_OUT
}

function join_rank() (
    set -exo pipefail
    _DATA_TYPE=$1
    _IN=$2
    _VERT=$3
    _OUT=$4
    _SORT_THREADS=${5:-2}

    _SORT_BUFFER=16g
    _SORT_COMPRESS="--compress-program=gzip"

    if [ -d $_VERT ]; then
        # _VERT is a directory with multiple vertices files
        _VERT="$_VERT/*.gz"
    fi

    ### unpack scores with LAW, join node names via paste,
    ### assign ranks on sorted lines by nl
	$LW it.unimi.dsi.law.io.tool.DataInput2Text --type $_DATA_TYPE $_IN - \
        | paste - <(zcat $_VERT | cut -f2) \
        | sort --parallel $_SORT_THREADS --batch-size $(($SORT_THREADS*2)) --buffer-size $_SORT_BUFFER $_SORT_COMPRESS -t$'\t' -k1,1gr --stable \
        | nl -w1 -nln \
        | gzip >$_OUT
)

function join_hc_pagerank() {
    NAME="$1"
    SORT_THREADS="$2"
    SORTOPTS="--parallel=$SORT_THREADS --batch-size=$(($SORT_THREADS*2)) --buffer-size=16g --compress-program=gzip"
    (echo -e "#hc_pos\t#hc_val\t#pr_pos\t#pr_val\t#host_rev";
     zcat $NAME/harmonic-centrality.txt.gz | perl -lpe 's/^(\d+) +(?=\t)/$1/g' | sort $SORTOPTS -t$'\t' -k3,3 --unique --stable \
         | join -a1 -a2 -e'---' -t$'\t' -j3 -o1.1,1.2,2.1,2.2,0 - \
                <(zcat $NAME/pagerank.txt.gz | perl -lpe 's/^(\d+) +(?=\t)/$1/g' | sort $SORTOPTS -t$'\t' -k3,3 --unique --stable) \
         | sort $SORTOPTS -t$'\t' -k1,1n -s) \
     | gzip >$NAME/ranks.txt.gz
}

function connected_distrib() {
    NUM_NODES=$1
    INPUT=$2
    OUTPUT=$3
    _SORT_THREADS=${5:-2}
    SORTOPTS="--parallel=$SORT_THREADS --batch-size=$(($SORT_THREADS*2)) --buffer-size=16g --compress-program=gzip"
    (echo -e "  #freq #size"; \
     $LW it.unimi.dsi.law.io.tool.DataInput2Text --type int $INPUT - | sort $SORTOPTS -nr | uniq -c \
         | perl -lpe 'if ($. <= 10) { /(\d+)$/; $_ .= sprintf("\t%2.2f%%", 100*$1/'$NUM_NODES') }') \
        | gzip >$OUTPUT
}

function degree_distrib() {
    TYPE="$1"
    NAME="$2"
    GRAPH="$3"
    (echo -e "#arcs\t#nodes";
     perl -lne 'print sprintf("%d\t%s", ($.-1), $_) if $_ ne 0' $NAME/$GRAPH.$TYPE) \
        | gzip >$NAME/$TYPE-distrib.txt.gz
}



set -exo pipefail

if [ -d $EDGES ]; then
    # edges is a directory with multiple files
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
        sort_input=""
        for e in $EDGES/part-*.gz; do
            sort_input="$sort_input <(zcat $e)"
        done
        _step bvgraph \
              bash -c "eval \"sort --batch-size 96 -t$'\t' -k1,1n -k2,2n --stable --merge $sort_input\" | $WG $WGP.BVGraph --once -g $WGP.ArcListASCIIGraph - $NAME/bvgraph"
    else
        _step bvgraph \
              $WG $WGP.BVGraph --threads $THREADS -g $WGP.ArcListASCIIGraph <(zcat $EDGES/*.gz) $NAME/bvgraph
    fi
else
    if ${USE_WEBGRAPH_BIG:-false}; then
        _step bvgraph \
              bash -c "zcat $EDGES | $WG $WGP.BVGraph --once -g $WGP.ArcListASCIIGraph - $NAME/bvgraph"
    else
        _step bvgraph \
              $WG $WGP.BVGraph --threads $THREADS -g $WGP.ArcListASCIIGraph <(zcat $EDGES) $NAME/bvgraph
    fi
fi

# if low memory, add
# --offline, combined with
# -Djava.io.tmpdir=... to point to a temporary directory with free space 2 times the graph size
if ${USE_WEBGRAPH_BIG:-false}; then
    _step transpose \
          $WG $WGP.Transform transposeOffline $NAME/bvgraph $NAME/bvgraph-t
else
    _step transpose \
          $WG $WGP.Transform transpose $NAME/bvgraph $NAME/bvgraph-t
fi
# _step symmetrize \
#       $WG $WGP.Transform symmetrize $NAME/bvgraph $NAME/bvgraph-t $NAME/bvgraph-sym

_step hyperball \
      $WG $WGP.algo.HyperBall --threads $THREADS --offline --log2m $HYPERBALL_REGISTERS \
      --harmonic-centrality $NAME/bvgraph-hc.bin $NAME/bvgraph-t $NAME/bvgraph

if ! ${USE_WEBGRAPH_BIG:-false}; then
	# page rank not supported for big graphs in LAW library
    _step pagerank \
          $LW it.unimi.dsi.law.rank.PageRankParallelGaussSeidel --mapped --expand --threads $THREADS $NAME/bvgraph-t $NAME/bvgraph-pagerank
fi

_step_bg connected 15 \
         $WG $WGP.algo.ConnectedComponents --threads $THREADS -m --sizes -t $NAME/bvgraph-t $NAME/bvgraph
_step_bg strongly-connected 15 \
         $WG $WGP.algo.StronglyConnectedComponents --sizes $NAME/bvgraph
_step_bg stats 15 \
         $WG $WGP.Stats --save-degrees $NAME/bvgraph


SORT_THREADS=$((($THREADS > 4) ? ($THREADS/2) : 2))
_step_bg join_hc 15 \
         join_rank float $NAME/bvgraph-hc.bin $VERTICES $NAME/harmonic-centrality.txt.gz $SORT_THREADS
if ! ${USE_WEBGRAPH_BIG:-false}; then
    _step_bg join_pr_gs 15 \
             join_rank double $NAME/bvgraph-pagerank.ranks $VERTICES $NAME/pagerank.txt.gz $SORT_THREADS
fi


# join ranks into one file
if ! ${USE_WEBGRAPH_BIG:-false}; then
    wait
    _step_bg join_hc_pagerank 60 \
             join_hc_pagerank $NAME $SORT_THREADS
fi

wait # until background processes are finished

NODES=$(perl -lne 'print if s@^nodes=@@' $NAME/bvgraph.stats)
_step connected_distrib \
      connected_distrib $NODES $NAME/bvgraph.wccsizes $NAME/connected-components-distrib.txt.gz $SORT_THREADS
_step strongly_connected_distrib \
      connected_distrib $NODES $NAME/bvgraph.sccsizes $NAME/strongly-connected-components-distrib.txt.gz $SORT_THREADS

_step indegree_distrib \
      degree_distrib indegree $NAME bvgraph
_step outdegree_distrib \
      degree_distrib outdegree $NAME bvgraph

wait # until background processes are finished
