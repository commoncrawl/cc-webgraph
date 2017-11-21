#!/bin/bash

if [ $# -lt 3 ]; then
	echo "$0 <number_of_vertices> <input_dir> <output_dir> [<tmp_dir>]" >&2
	exit 1
fi

SIZE="$1"
INPUTDIR="$2"
OUTPUTDIR="$3"
TMPDIR=${4:-./tmp/}

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
SORTOPTS="--batch-size 128 --buffer-size 32g --parallel=16 --temporary-directory $TMPDIR --compress-program=gzip"

set -exo pipefail

test -d $TMPDIR || mkdir $TMPDIR

mkdir -p $OUTPUTDIR/domain

java -Xmx60g -cp target/cc-webgraph-0.1-SNAPSHOT-jar-with-dependencies.jar \
     org.commoncrawl.webgraph.HostToDomainGraph \
     $SIZE \
     <(zcat $INPUTDIR/vertices/part-*.gz | sed -e 's/$/./' | sort $SORTOPTS -t$'\t' -k2,2 | sed -e 's/\.$//') \
     >(gzip >$OUTPUTDIR/domain/vertices.txt.gz) \
     <(zcat $INPUTDIR/edges/part-*.gz) \
     >(sort $SORTOPTS -t$'\t' -k1,1n -k2,2n -s -u | gzip >$OUTPUTDIR/domain/edges.txt.gz)


