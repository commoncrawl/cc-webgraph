#!/bin/bash

export LC_ALL=C

source "$(dirname $0)"/webgraph_config.sh

DIR=$(dirname $0)/webgraph-big-$WEBGRAPH_BIG_VERSION

CLASSPATH=$DIR/webgraph-big-$WEBGRAPH_BIG_VERSION.jar:$(ls $DIR/deps/*.jar | tr '\n' ':')

if ! echo "$JAVA_OPTS" | grep -qE -e "-Xmx[0-9]+"; then
    # heuristics to run webgraph with 80% of available RAM (or all RAM - 8 GB if this is larger)
    MEMMB=$(free -m | perl -ne 'do { $p80 = int($1*.8); $a8 = int($1-8192); $m = $p80; $m = $a8 if $a8 > $p80; print $m; last } if /(\d+)/')
    JAVA_OPTS="$JAVA_OPTS -Xmx${MEMMB}m"
fi


case "$1" in
    it.unimi.dsi.big.webgraph.algo.HyperBall )
        JAVA_OPTS="$JAVA_OPTS -server -Xss256K -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=$(($MEMMB/3))m \
          -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB \
          -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=99 -XX:+UseCMSInitiatingOccupancyOnly \
          -verbose:gc -Xloggc:gc.log"
    ;;
esac

set -x
time $JAVA_HOME/bin/java $JAVA_OPTS -cp $CLASSPATH "$@"




