#!/bin/bash

# Build node indexes to interactively explore a Common Crawl webgraph.
# The webgraph files are expected to be placed in the current directory.

NAME="$1"
VERTICES="$2"
if ! shift 2; then
    echo "$(basename $0) <name> <vertices>"
    echo
    echo "Build node indexes to interactively explore a Common Crawl webgraph."
    echo "The webgraph files are expected to be placed in the current directory."
    echo
    echo "  <name>      basename of the graph (without the .graph suffix)"
    echo "  <vertices>  vertices file name (including the file suffix)"
    echo "              or directory containing the vertices files"
    echo
    exit 1
fi

export LC_ALL=C

BIN="$(dirname $0)"
WG="$BIN/run_webgraph.sh"

declare -A suffix_name_map
suffix_name_map=(
    graph      "webgraph / BVGraph"
    properties "webgraph properties"
    offsets    "webgraph offsets"
    iepm "immutable external prefix map"
    mph  "minimal perfect hash"
    fcl  "front coded list"
    smph "string map perfect hash"
)

function list_webgraph_files() {
    name="$1"; shift
    ok=true
    for suffix in "$@"; do
        if [ -e $name.$suffix ]; then
            printf " .%-10s : %-20s  (%s)\n" "$suffix" \
                   "${suffix_name_map[$suffix]}" "$name.$suffix"
        else
            echo -e "Missing $name.$suffix (${suffix_name_map[$suffix]})"
            ok=false
        fi
    done
    if ! $ok; then
        exit 1
    fi
}

function index_status() {
    echo
    echo "Prepared webgraph $NAME for look-ups by node label."
    echo "The following files (by file suffix) will be used:"

    echo "Webgraph:"
    list_webgraph_files $NAME graph properties offsets
    echo "Webgraph (transpose):"
    list_webgraph_files $NAME-t graph properties offsets

    echo "Mapping vertex labels to vertex IDs:"
    if [ -e $NAME.iepm ]; then
        list_webgraph_files $NAME iepm
    else
        list_webgraph_files $NAME mph fcl smph
    fi
}


# check for graph files (.graph and .properties), also for the
# transpose of the graph ($NAME-t.$suffix)
echo "Verifying webgraph files:"
list_webgraph_files $NAME graph properties
echo "Verifying webgraph files (transpose of the graph):"
list_webgraph_files $NAME-t graph properties

# check for the vertices file
if ! [ -e $VERTICES ]; then
    echo "Vertices file not found"
    exit 1
fi


# generate offsets (*.offsets and *.obl)
if ! [ -e $NAME.offsets ]; then
    "$WG" it.unimi.dsi.webgraph.BVGraph --offsets --list $NAME
    echo "webgraph offsets file created"
fi
if ! [ -e $NAME-t.offsets ]; then
    "$WG" it.unimi.dsi.webgraph.BVGraph --offsets --list $NAME-t
    echo "webgraph offsets file created (transpose of the graph)"
fi


# building `iepm` "immutable external prefix map"
# (https://dsiutils.di.unimi.it/docs/it/unimi/dsi/util/ImmutableExternalPrefixMap.html)
# bidirectional mapping from node names to node IDs
if [ -e $NAME.iepm ]; then
    index_status
    exit 0
fi
CAT_VERTICES="zcat $VERTICES"
if [ -d $VERTICES ]; then
    # host-level webgraph, multiple vertex files
    CAT_VERTICES="zcat $VERTICES/*.txt.gz"
fi
if (set -eo pipefail;
    eval $CAT_VERTICES \
        | cut -f2 \
        | "$WG" it.unimi.dsi.util.ImmutableExternalPrefixMap --block-size 4Ki $NAME.iepm); then
    echo "immutable external prefix map successfully built: $NAME.iepm"
    index_status
    exit 0
fi
# Note: building the `iepm` may fail for older versions of the domain
# graph (before the graphs of May, June/July and August 2022) because
# the nodes were not properly lexicographically sorted while folding
# host names to domain names. If this is the case, continue to create
# instead mappings which do not depend on proper sorting.

# build
# - the `mph` (minimal perfect hash) file mapping from node label
#   (reversed domain name) to node ID
# - a front coded list to map node IDs to node labels
if ! [ -e $NAME.mph ] || ! [ -e $NAME.fcl ]; then
    zcat $VERTICES \
        | cut -f2 \
        | tee >("$WG" it.unimi.dsi.sux4j.mph.GOV4Function $NAME.mph) \
        | "$WG" it.unimi.dsi.util.FrontCodedStringList --utf8 --ratio 32 $NAME.fcl
fi

# build the `smph` file (string map perfect hash) required to
# determine whether a node label is present in the `mph` file
if ! [ -e $NAME.smph ]; then
    zcat $VERTICES \
        | cut -f2 \
        | "$WG" it.unimi.dsi.util.ShiftAddXorSignedStringMap $NAME.mph $NAME.smph
fi


index_status
