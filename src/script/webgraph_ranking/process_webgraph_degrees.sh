#!/bin/bash

set -eo pipefail

NAME="$1"
TYPE="${2:-domain}"

if [ -z "$NAME" ]; then
    echo "Usage: $(basename $0) <graph-name> [<type>]"
    echo -e "\tgraph-name\tbase name of the webgraph (without the file suffix .graph)"
    echo -e "\ttype\ttype (level) of the graph aggregation: domain (default) or host"
    exit 1
fi

WG=$(dirname $0)/run_webgraph.sh

if [ -e $NAME.outdegrees ] && [ -e $NAME.indegrees ]; then
    : # out/indegrees already done
else
    $WG it.unimi.dsi.webgraph.Stats --save-degrees "$NAME"
fi


if [ "$TYPE" == "domain" ]; then
    zcat $NAME-vertices.txt.gz
else
    zcat vertices/*.txt.gz
fi \
    | cut -f2- \
    | paste $NAME.outdegrees $NAME.indegrees - \
    | gzip >$NAME-outdegrees-indegrees.txt.gz


HEADER="outdegree\tindegree\tname"
if [ "$TYPE" == "domain" ]; then
    HEADER="outdegree\tindegree\tname\tnumsubdomains"
fi

(echo -e "$HEADER";
 set +o pipefail;
 zcat $NAME-outdegrees-indegrees.txt.gz \
     | perl -aF'\t' -lne 'print if $F[0] > 1000' \
     | sort -k1,1nr \
     | head -10000) \
    | gzip >$NAME-outdegrees-indegrees-topout.txt.gz

(echo -e "$HEADER";
 set +o pipefail;
 zcat $NAME-outdegrees-indegrees.txt.gz \
     | perl -aF'\t' -lne 'print if $F[1] > 1000' \
     | sort -k2,2nr \
     | head -10000) \
    | gzip >$NAME-outdegrees-indegrees-topin.txt.gz

