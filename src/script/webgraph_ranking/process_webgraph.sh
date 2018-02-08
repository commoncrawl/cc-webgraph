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
test -d $OUTPUTDIR/logs || mkdir $OUTPUTDIR/logs
LOGDIR=$OUTPUTDIR/logs
# file to stop workflow
STOP_FILE_=$LOGDIR/$(basename $0 .sh).stop

VERTICES_FIELDS=${VERTICES_FIELDS:-2}


function join_rank() (
    set -exo pipefail
    _DATA_TYPE=$1
    _IN=$2
    _VERT=$3
    _OUT=$4
    _SORT_THREADS=${5:-2}
    _EXTRA_FIELDS=""
    if [ -n "$6" ]; then
        _EXTRA_FIELDS=",$6"
    fi

    _SORT_BUFFER=16g
    _SORT_COMPRESS="--compress-program=gzip"

    if [ -d $_VERT ]; then
        # _VERT is a directory with multiple vertices files
        _VERT="$_VERT/*.gz"
    fi

    ### unpack scores with LAW, join node names via paste,
    ### assign ranks on sorted lines by nl
	$LW it.unimi.dsi.law.io.tool.DataInput2Text --type $_DATA_TYPE $_IN - \
        | paste - <(zcat $_VERT | cut -f2$_EXTRA_FIELDS) \
        | sort --parallel $_SORT_THREADS --batch-size $(($_SORT_THREADS*4)) --buffer-size $_SORT_BUFFER $_SORT_COMPRESS -t$'\t' -k1,1gr --stable \
        | nl -w1 -nln \
        | gzip >$_OUT
)

function join_harmonicc_pagerank() {
    NAME="$1"
    _SORT_THREADS="$2"
    _IN_HC="$3"
    _IN_PR="$4"
    _OUT="$5"
    _EXTRA_FIELDS=""
    if [ -n "$6" ]; then
        _EXTRA_FIELDS=",$6"
        _EXTRA_FIELDS_HEADER="\t$7"
    fi
    SORTOPTS="--parallel=$_SORT_THREADS --batch-size=$(($_SORT_THREADS*4)) --buffer-size=16g --compress-program=gzip"
    (echo -e "#harmonicc_pos\t#harmonicc_val\t#pr_pos\t#pr_val\t#host_rev$_EXTRA_FIELDS_HEADER";
     zcat $_IN_HC | sort $SORTOPTS -t$'\t' -k3,3 --unique --stable \
         | join -a1 -a2 -e'---' -t$'\t' -j3 -o1.1,1.2,2.1,2.2,0$_EXTRA_FIELDS - \
                <(zcat $_IN_PR | sort $SORTOPTS -t$'\t' -k3,3 --unique --stable) \
         | sort $SORTOPTS -t$'\t' -k1,1n -s) \
     | gzip >$_OUT
}

function connected_distrib() {
    NUM_NODES=$1
    INPUT=$2
    OUTPUT=$3
    _SORT_THREADS=${5:-2}
    SORTOPTS="--parallel=$_SORT_THREADS --batch-size=$(($_SORT_THREADS*4)) --buffer-size=16g --compress-program=gzip"
    (echo -e "  #freq #size"; \
     $LW it.unimi.dsi.law.io.tool.DataInput2Text --type int $INPUT - | sort $SORTOPTS -nr | uniq -c \
         | perl -lpe 'if ($. <= 10) { /(\d+)$/; $_ .= sprintf("\t%2.2f%%", 100*$1/'$NUM_NODES') }') \
        | gzip >$OUTPUT
}

function degree_distrib() {
    TYPE="$1"
    NAME="$2"
    (echo -e "#arcs\t#nodes";
     perl -lne 'print sprintf("%d\t%s", ($.-1), $_) if $_ ne 0' $NAME.$TYPE) \
        | gzip >$FULLNAME-$TYPE-distrib.txt.gz
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
              bash -c "eval \"sort --batch-size 96 -t$'\t' -k1,1n -k2,2n --stable --merge $sort_input\" | $WG $WGP.BVGraph --once -g $WGP.ArcListASCIIGraph - $FULLNAME"
    else
        _step bvgraph \
              $WG $WGP.BVGraph --threads $THREADS -g $WGP.ArcListASCIIGraph <(zcat $EDGES/*.gz) $FULLNAME
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

# if low memory, add
# --offline, combined with
# -Djava.io.tmpdir=... to point to a temporary directory with free space 2 times the graph size
if ${USE_WEBGRAPH_BIG:-false}; then
    _step transpose \
          $WG $WGP.Transform transposeOffline $FULLNAME $FULLNAME-t
else
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
          $LW it.unimi.dsi.big.law.rank.PageRankParallelGaussSeidel      --mapped --threads $THREADS $FULLNAME-t $FULLNAME-pagerank
else
    _step pagerank \
          $LW it.unimi.dsi.law.rank.PageRankParallelGaussSeidel --expand --mapped --threads $THREADS $FULLNAME-t $FULLNAME-pagerank
fi

_step_bg connected 15 \
         $WG $WGP.algo.ConnectedComponents --threads $THREADS -m --renumber --sizes -t $FULLNAME-t $FULLNAME
_step_bg strongly-connected 15 \
         $WG $WGP.algo.StronglyConnectedComponents --renumber --sizes $FULLNAME

SORT_THREADS=$((($THREADS > 4) ? ($THREADS/2) : 2))
EXTRA_FIELDS=""
EXTRA_FIELDS_JOIN=""
if [ $VERTICES_FIELDS -gt 2 ]; then
    EXTRA_FIELDS="3-$VERTICES_FIELDS"
    EXTRA_FIELDS_JOIN="1.4"
    for i in $(seq 4 $VERTICES_FIELDS); do
        EXTRA_FIELDS_JOIN="${EXTRA_FIELDS_JOIN},1.$(($i+1))"
    done
fi
_step_bg join_harmonicc 15 \
         join_rank float $FULLNAME-harmonicc.bin $VERTICES $FULLNAME-harmonic-centrality.txt.gz $SORT_THREADS "$EXTRA_FIELDS"
_step_bg join_pr_gs 15 \
         join_rank double $FULLNAME-pagerank.ranks $VERTICES $FULLNAME-pagerank.txt.gz $SORT_THREADS

wait # until background processes are finished

# stats use connected components files, wait for these to be finished
_step_bg stats 60 \
         $WG $WGP.Stats --save-degrees $FULLNAME

# join ranks into one file
_step_bg join_harmonicc_pagerank 60 \
         join_harmonicc_pagerank $NAME $SORT_THREADS $FULLNAME-harmonic-centrality.txt.gz $FULLNAME-pagerank.txt.gz $FULLNAME-ranks.txt.gz "$EXTRA_FIELDS_JOIN" "#n_hosts"


NODES=$(perl -lne 'print if s@^nodes=@@' $FULLNAME.stats)
_step connected_distrib \
      connected_distrib $NODES $FULLNAME.wccsizes $FULLNAME-connected-components-distrib.txt.gz $SORT_THREADS
# TODO: should use *.sccdistr
_step strongly_connected_distrib \
      connected_distrib $NODES $FULLNAME.sccsizes $FULLNAME-strongly-connected-components-distrib.txt.gz $SORT_THREADS

_step indegree_distrib \
      degree_distrib indegree $FULLNAME outdegree_distrib \
      degree_distrib outdegree $FULLNAME

wait # until background processes are finished
