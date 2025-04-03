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

USING_CURL=false
USING_WGET=false
if command -v curl &>/dev/null; then
    USING_CURL=true
elif command -v wget &>/dev/null; then
    USING_WGET=true
else
    echo "Either curl or wget are required for downloading" >&2
    exit 1
fi

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
        elif [ -d "$name" ] && [[ "$suffix" =~ ^\*. ]]; then
            ls "$name"/* | sed 's/^/\t/'
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

    if $USING_CURL; then

        curl --silent --show-error --fail \
             --remote-time -o "$FILE" --time-cond "$FILE" --continue-at - \
             --retry 1000 --retry-delay 1 "$URL"

    elif $USING_WGET; then

        if [ "$(dirname "$FILE")" == "." ]; then
            wget --continue --timestamping --tries=0 --retry-on-http-error=503 --waitretry=1 "$URL"
        else
            wget --continue --timestamping --directory-prefix="$(dirname "$FILE")" \
                 --tries=0 --retry-on-http-error=503 --waitretry=1 "$URL"
        fi

    fi
}

function download_files() {
    name="$1"; shift
    for suffix in "$@"; do
        download_file "$name.$suffix"
    done
}


BASE_NAME="${NAME%-domain}"
BASE_NAME="${BASE_NAME%-host}"
GRAPH_AGGR_LEVEL="${NAME##*-}"


set -e # stop on errors

download_files "$NAME" graph properties stats
download_files "$NAME-t" graph properties

if [ "$GRAPH_AGGR_LEVEL" == "domain" ]; then
    download_files "$NAME-vertices" txt.gz
else
    download_files "$NAME-vertices" paths.gz
    zcat "$NAME-vertices".paths.gz \
        | while read path; do
        file=${path#projects/hyperlinkgraph/$BASE_NAME/$GRAPH_AGGR_LEVEL/}
        mkdir -p $(dirname "$file")
        download_file "$file"
     done
fi

echo "Downloaded files"
echo "- webgraph"
list_webgraph_files $NAME graph properties stats
echo "- webgraph (transpose)"
list_webgraph_files $NAME-t graph properties
echo "- webgraph vertices"
if [ "$GRAPH_AGGR_LEVEL" == "domain" ]; then
    list_webgraph_files $NAME-vertices txt.gz
else
    list_webgraph_files vertices "*.txt.gz"
fi
