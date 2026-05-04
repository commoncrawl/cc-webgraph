#!/bin/bash

# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2022 Common Crawl and contributors

FLAGS=()
PROPERTIES=()
while true; do
    case "$1" in
        "-D"* )
            PROPERTIES=("${PROPERTIES[@]}" "$1")
            shift
            ;;
        "--aggregation-level" )
            FLAGS=("${FLAGS[@]}" "$1")
            shift
            # takes one argument
            FLAGS=("${FLAGS[@]}" "$1")
            shift
            ;;
        "-"* )
            FLAGS=("${FLAGS[@]}" "$1")
            shift
            ;;
        * )
            break
            ;;
    esac
done

JAR=target/cc-webgraph-0.1-SNAPSHOT-jar-with-dependencies.jar

if [ $# -lt 3 ]; then
	echo "$0 [<flags>...] <number_of_vertices> <input_dir> <output_dir> [<tmp_dir>]" >&2
    if [ ${#FLAGS[@]} -gt 0 ]; then
        echo ""
        echo "Calling HostToDomainGraph with provided flags (${FLAGS[*]}):"
        "$JAVA_HOME"/bin/java -cp "$CLASSPATH":"$JAR" "${PROPERTIES[@]}" \
                    org.commoncrawl.webgraph.HostToDomainGraph "${FLAGS[@]}"
    fi
	exit 1
fi

SIZE="$1"
INPUTDIR="$2"
OUTPUTDIR="$3"
TMPDIR=${4:-./tmp/}

MAIN_MEM_GB=16
PARALLEL_SORT_THREADS=2

# Reduce host-level web graph to domain-level graph
# - running HostToDomainGraph which has low memory requirements
# - requires properly sorted input:
#    * reversed host names
#    * all hosts/subdomains of one domain following in a single input block
# - approx. memory requirements (see below JXMX):
#    * for graphs with less than 2^31 vertices
#       4 GB +  4*number_of_vertices Bytes
#    * larger graphs
#       8 GB + 10*number_of_vertices Bytes
#
# Notes about input sorting:
#
# 1 C locale is mandatory to keep reversed hosts of one domain or top-level domain
#   together in a single block:
#     echo -e "com.opus\ncom.opera\nco.mopus\nco.mopera" | shuf | LC_ALL=en_US.utf8 sort
#     co.mopera
#     com.opera
#     com.opus
#     co.mopus
#   vs.
#     echo -e "com.opus\ncom.opera\nco.mopus\nco.mopera" | shuf | LC_ALL=C sort
#     co.mopera
#     co.mopus
#     com.opera
#     com.opus
#   This requirement is met by the output of the cc-pyspark job.
#
# In an older version, the input was re-sorted to try to group
# domains and their subdomains together:
#
# 2 Sorting with C locale, places a hyphen (valid in host and subdomain names)
#   before a dot:
#     ac.gov
#     ac.gov.ascension
#     ac.gov.ascension-island
#     ac.gov.ascension.mail
#   This causes that the domain "ac.gov.ascension" and its subdomain "ac.gov.ascension.mail"
#   end up in two separated blocks of the input, even with sorting using the C locale.
#
#   The initial solution to ensure that the subdomains of "ac.gov.ascension" are not split
#   into two blocks, was to add an artificial dot temporarily to the end of each host
#   name during sorting:
#     zcat vertices.txt.gz | sed -e 's/$/./' \
#        | sort $SORTOPTS -t$'\t' -k2,2 | sed -e 's/\.$//'
#   The domain name "ac.gov.ascension" in the example above becomes temporarily
#   "ac.gov.ascension." and is now sorted after "ac.gov.ascension-island."
#
#   A sort order that keeps hosts/domains of a common suffix in one block can be
#   also achieved if dots are replaced by commas:
#     zcat vertices.txt.gz | tr . , \
#        | sort $SORTOPTS -t$'\t' -k2,2 | tr , .
#   This approach is utilized by the "Sort-friendly URI Reordering Transform" (SURT),
#   see <http://crawler.archive.org/articles/user_manual/glossary.html#surt>.
#
# However, the public suffix list adds a further issue, which makes it impossible
# to group domains and subdomains together, by simply sorting the input:
#
# 3 There are multi-part suffixes, such as "co.uk" (or "uk.co" in reverse domain name
#   notation). And the suffixes of a multi-part suffix can be public suffixes themselves:
#   also "uk" is a public suffix. But they do not need to. For example: "no" and
#   "os.hordaland.no" are in the public suffix list but "hordaland.no" is not.
#   In this situation, adding a trailing dot does not even guarantee that all hosts of
#   a domain under a public suffix are in a contiguous block:
#
#     $> cat hordaland.txt
#     no.hordaland
#     no.hordaland-teater
#     no.hordaland.os
#     no.hordaland.os.bibliotek
#     no.hordaland.oygarden
#     no.hordalandfolkemusikklag
#
#     $> cat hordaland.txt | sed 's/$/./' | LC_ALL=C sort
#     no.hordaland-teater.
#     no.hordaland.
#     no.hordaland.os.
#     no.hordaland.os.bibliotek.
#     no.hordaland.oygarden.
#     no.hordalandfolkemusikklag.
#
#   The host names "no.hordaland." and "no.hordaland.oygarden." both
#   are under the domain ""no.hordaland" (public suffix is "no").
#
# To address this issue (point 3), the HostToDomainGraph class now caches
# potentially "missorted" candidates and processes them later together
# with the related subdomains / host names.
#
# 4 This also addresses the fact, that re-sorting billions of input lines is
#   computationally expensive.
#
# Output sorting:
#
# 5 Ideally, the domain output should be lexicographically sorted
#   as well. This is a requirement to store the map of node names and IDs
#   in an "immutable external prefix map" (IEPM).
#   If a trailing dot is added and then removed (and no cache is used), the
#   output sorting would be consequently the same as if there is a trailing dot:
#     ac.gov.ascension-island.
#     ac.gov.ascension.
#   respectively (after removing the trailing dot)
#     ac.gov.ascension-island
#     ac.gov.ascension
#
#   The required ASCII sorting is:
#     ac.gov.ascension
#     ac.gov.ascension-island
#
#   We cannot re-sort the output because this would also require to change
#   the node IDs because the WebGraph framework expects the arc/edge input
#   to be numerically sorted. And the vertices/nodes are enumerated as they
#   are sorted, i.e. node IDs are line numbers starting with zero.
#
#   Note: The approach to replace dots by commas ensures proper lexicographic
#   sorting even if the replacement is inverted. However, it does not guarantee
#   that all domains of one suffix are in a contigous block, if that suffix
#   is a suffix of another suffix. See point 3.
#
# Please see https://github.com/commoncrawl/cc-webgraph/issues/3
# and https://github.com/commoncrawl/cc-webgraph/issues/33
# for further details.
#

export LC_ALL=C

# sort with large buffers, merge sort over many files if possible
SORTOPTS="--batch-size 128 --buffer-size $((1+MAIN_MEM_GB/5))g --parallel=$PARALLEL_SORT_THREADS --temporary-directory $TMPDIR" # --compress-program=gzip

BIN=$(dirname $0)
source $BIN/workflow_lib.sh

LOG__ "Starting aggregation of host-level graph on the domain level"

set -exo pipefail

test -d "$TMPDIR" || mkdir "$TMPDIR"


_EDGES=$INPUTDIR/edges.txt.gz
if [ -e "$_EDGES" ]; then
    echo "Found single edges file: $_EDGES"
elif [ -d "$INPUTDIR"/edges/ ]; then
    # edges is a directory with multiple edges files
    _EDGES="$INPUTDIR/edges/*.gz"
    echo "Found edges directory, using: $_EDGES"
else
    echo "Input edges file(s) not found"
    exit 1
fi

_VERTICES=$INPUTDIR/vertices.txt.gz
if [ -e "$_VERTICES" ]; then
    echo "Found single vertices file: $_VERTICES"
elif [ -d "$INPUTDIR"/vertices/ ]; then
    # vertices is a directory with multiple vertices files
    echo "Found vertices directory, using: $_VERTICES"
    _VERTICES="$INPUTDIR/vertices/*.gz"
else
    echo "Input vertices file(s) not found"
    exit 1
fi


mkdir -p "$OUTPUTDIR/"

# Heuristics to set Java heap size:
# - hold the array mapping host IDs to domain IDs
#   - 4 bytes per vertex for host graphs with less than 2 billion nodes
#   - 8 bytes per vertex for larger graphs
#   - to be on the safe side: calculate with 5 resp. 10 bytes per node
# - plus memory for buffering domain names until all hosts of one domain
#   are processed and the domain names can be written to output.
#   The buffering is required to deal with sorting issue: it's not possible
#   to sort the list of reversed host names in a way that all hosts of one
#   domain are in a contiguous block. See the above explanations.
JXMX=$((4+1+5*SIZE/2**30))
if [ "$SIZE" -gt $((2**31-1024)) ]; then
    JXMX=$((8+1+10*SIZE/2**30))
fi

"$JAVA_HOME"/bin/java -Xmx${JXMX}g -cp "$CLASSPATH":"$JAR" \
                    "${PROPERTIES[@]}" \
                    org.commoncrawl.webgraph.HostToDomainGraph \
                    "${FLAGS[@]}" \
                    $SIZE \
                    <(zcat $_VERTICES) \
                    >(gzip >"$OUTPUTDIR"/vertices.txt.gz) \
                    <(zcat $_EDGES) \
                    >(sort $SORTOPTS -t$'\t' -k1,1n -k2,2n -s -u | gzip >"$OUTPUTDIR"/edges.txt.gz)

LOG__ "Waiting for data to be written to disk..."
wait # for subshells to finish

LOG__ "Finished aggregation of host-level graph on the domain level:"
ls -l "$OUTPUTDIR"/{vertices,edges}.txt.gz
