#!/bin/bash

if [ $# -lt 3 ]; then
	echo "$0 <number_of_vertices> <input_dir> <output_dir> [<tmp_dir>]" >&2
	exit 1
fi

SIZE="$1"
INPUTDIR="$2"
OUTPUTDIR="$3"
TMPDIR=${4:-./tmp/}

MAIN_MEM_GB=16
PARALLEL_SORT_THREADS=2
#CMPRS_TMP=false

# Reduce host-level web graph to domain-level graph
# - running HostToDomainGraph which has low memory requirements
# - requires properly sorted input:
#    * reversed host names
#    * all hosts/subdomains of one domain following in a single input block
# - approx. memory requirements:
#    * for graphs with less than 2^31 vertices
#       2 GB +  4*number_of_vertices Bytes
#    * larger graphs
#       8 GB + 10*number_of_vertices Bytes

# Notes about input sorting:
# - unfortunately the output of the cc-pyspark job does not completely meet
#   the required sorting criteria, so we need to resort the vertices of the
#   host graph
# - C locale is mandatory to keep reversed hosts of one domain or top-level domain
#   together in a single block:
#     echo -e "com.opus\ncom.opera\nco.mopus\nco.mopera" | shuf | LC_ALL=en_US.utf8 sort
#   vs.
#     echo -e "com.opus\ncom.opera\nco.mopus\nco.mopera" | shuf | LC_ALL=C sort
# - the second problem stems from the fact that a hyphen (valid in host and
#   subdomain names) is sorted before the dot:
#     ac.gov
#     ac.gov.ascension
#     ac.gov.ascension-island
#     ac.gov.ascension.mail
#   One solution to ensure that the subdomains of "ac.gov.ascension" are not split
#   into two blocks, is to add an artificial dot needs temporarily to the end of
#   each host name before sorting. 


export LC_ALL=C

# sort with large buffers, merge sort over many files if possible
SORTOPTS="--batch-size 128 --buffer-size $((1+$MAIN_MEM_GB/3))g --parallel=$PARALLEL_SORT_THREADS --temporary-directory $TMPDIR --compress-program=gzip"

set -exo pipefail

test -d $TMPDIR || mkdir $TMPDIR


_EDGES=$INPUTDIR/edges.txt.gz
if ! [ -e $_EDGES ] && [ -d $INPUTDIR/edges/ ]; then
    # edges is a directory with multiple edges files
    _EDGES="$INPUTDIR/edges/*.gz"
else
    echo "Input edges file(s) not found"
    exit 1
fi

if ! [ -e $INPUTDIR/vertices-sortdomain.txt.gz ]; then

    _VERTICES=$INPUTDIR/vertices.txt.gz
    if ! [ -e $_VERTICES ] && [ -d $INPUTDIR/vertices/ ]; then
        # vertices is a directory with multiple vertices files
        _VERTICES="$INPUTDIR/vertices/*.gz"
    else
        echo "Input vertices file(s) not found"
        exit 1
    fi

    zcat $_VERTICES | sed -e 's/$/./' \
        | sort $SORTOPTS -t$'\t' -k2,2 | sed -e 's/\.$//' \
        | gzip >$INPUTDIR/vertices-sortdomain.txt.gz
fi


mkdir -p $OUTPUTDIR/

JXMX=$((2+1+4*$SIZE/2**30))
if [ "$SIZE" -gt $((2**31-1024)) ]; then
    JXMX=$((8+1+10*$SIZE/2**30))
fi

java -Xmx${JXMX}g -cp target/cc-webgraph-0.1-SNAPSHOT-jar-with-dependencies.jar \
     org.commoncrawl.webgraph.HostToDomainGraph \
     -c \
     $SIZE \
     <(zcat $INPUTDIR/vertices-sortdomain.txt.gz) \
     >(gzip >$OUTPUTDIR/vertices.txt.gz) \
     <(zcat $_EDGES) \
     >(sort $SORTOPTS -t$'\t' -k1,1n -k2,2n -s -u | gzip >$OUTPUTDIR/edges.txt.gz)


