#!/bin/bash

source "$(dirname $0)"/webgraph_config.sh

set -e
set -x

mkdir -p download
cd download

WGET="wget --timestamping"
$WGET http://webgraph.di.unimi.it/webgraph-$WEBGRAPH_VERSION-src.tar.gz
$WGET http://webgraph.di.unimi.it/webgraph-$WEBGRAPH_VERSION-bin.tar.gz
$WGET http://webgraph.di.unimi.it/webgraph-deps.tar.gz

$WGET http://webgraph.di.unimi.it/webgraph-big-$WEBGRAPH_BIG_VERSION-bin.tar.gz
$WGET http://webgraph.di.unimi.it/webgraph-big-$WEBGRAPH_BIG_VERSION-src.tar.gz
$WGET http://webgraph.di.unimi.it/webgraph-big-deps.tar.gz

$WGET http://law.di.unimi.it/software/download/law-$LAW_VERSION-src.tar.gz
$WGET http://law.di.unimi.it/software/download/law-$LAW_VERSION-bin.tar.gz
$WGET http://law.di.unimi.it/software/download/law-deps.tar.gz

cd ..

tar xvfz download/webgraph-$WEBGRAPH_VERSION-bin.tar.gz
cd webgraph-$WEBGRAPH_VERSION
mkdir -p deps
cd deps
tar xvfz ../../download/webgraph-deps.tar.gz
cd ../..

tar xvfz download/webgraph-big-$WEBGRAPH_BIG_VERSION-bin.tar.gz
cd webgraph-big-$WEBGRAPH_BIG_VERSION
mkdir -p deps
cd deps
tar xvfz ../../download/webgraph-big-deps.tar.gz
cd ../..

tar xvfz download/law-$LAW_VERSION-bin.tar.gz
cd law-$LAW_VERSION
mkdir -p deps
cd deps
tar xvfz ../../download/law-deps.tar.gz
cd ../..
