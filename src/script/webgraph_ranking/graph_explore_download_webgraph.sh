#!/bin/bash

NAME="$1"
if ! shift 1; then
    echo "$(basename $0) <name>"
    echo
    echo "Download all files required to interactively explore a Common Crawl webgraph."
    echo "The downloaded files are placed in the current directory."
    echo "Wget or curl are required for downloading"
    echo
    echo "<name>   webgraph base name without file suffix, eg. cc-main-2023-mar-may-oct-domain"
    echo
    exit 1
fi

export LC_ALL=C

BIN="$(dirname $0)"

declare -A suffix_name_map
suffix_name_map=(
    graph      "webgraph / BVGraph"
    properties "webgraph properties"
    offsets    "webgraph offsets"
    stats      "webgraph statistics"
    txt.gz     "text file (vertex labels)"
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

function download_file() {
    FILE="$1"
    if [ -e "$FILE" ]; then
        return # already done
    fi
    URL="https://data.commoncrawl.org/projects/hyperlinkgraph/$BASE_NAME/$GRAPH_AGGR_LEVEL/$FILE"
    echo "Downloading $URL"
    # wget --continue --timestamping "$URL"
    curl --silent --remote-time -o "$FILE" --time-cond "$FILE" --continue-at - "$URL"
}

function download_files() {
    name="$1"; shift
    for suffix in "$@"; do
        download_file "$name.$suffix"
    done
}


BASE_NAME="${NAME%-domain}"
GRAPH_AGGR_LEVEL="${NAME##*-}"


download_files "$NAME" graph properties stats
download_files "$NAME-vertices" txt.gz
download_files "$NAME-t" graph properties

echo "Downloaded files"
echo "- webgraph"
list_webgraph_files $NAME graph properties stats
echo "- webgraph (transpose)"
list_webgraph_files $NAME-t graph properties
echo "- webgraph vertices"
list_webgraph_files $NAME-vertices txt.gz

