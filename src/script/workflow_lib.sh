# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2022 Common Crawl and contributors

### functions used to run webgraph... workflow

function LOG__() {
    echo $(date '+[%Y-%m-%d %H:%M:%S]') "$@"
}

function _test_step() {
    if [ -n "$STOP_FILE_" ] && [ -e "$STOP_FILE_" ]; then
        LOG__ INFO "Found stop file: $STOP_FILE_"
        exit 0
    fi
    _STEP__="$1"; shift
    if [ -e "$LOGDIR"/"$_STEP__".log ] \
           || [ -e "$LOGDIR"/"$_STEP__".log.xz ] \
           || [ -e "$LOGDIR"/"$_STEP__".log.gz ] \
           || [ -e "$LOGDIR"/"$_STEP__".log.bz2 ]; then
        LOG__ INFO "Step $_STEP__ already done, $LOGDIR/$_STEP__.log exists"
        return 1
    fi
    return 0
}

function _step() {
    _STEP__="$1"; shift
    if _test_step "$_STEP__"; then
        LOG__ INFO "Running step $_STEP__ ..."
        if "$@" &>"$LOGDIR"/"$_STEP__".log; then
             LOG__ INFO "Step $_STEP__ succeeded."
        else
            RES=$?
            LOG__ ERROR "Step $_STEP__ failed with $RES"
            mv "$LOGDIR"/"$_STEP__".log "$LOGDIR"/"$_STEP__".failed.$(date +%Y-%m-%d-%H-%M-%S).log
            LOG__ ERROR "Exiting ..."
            exit $RES
        fi
    fi
}

function _step_bg() {
    _STEP__="$1"
    _SLEEP_="$2"
    shift 2
    LOG__ INFO "Running background step $_STEP__ ..."
    if ! [ "$_SLEEP_" -eq "$_SLEEP_" ] 2>/dev/null; then
        echo "_step_bg <name> <sleep> <command>..."
        echo " parameter <sleep> must be an integer"
        echo " (sleep seconds after launching command, before executing next step)"
        exit 1
    fi
    if _test_step "$_STEP__"; then
        _step "$_STEP__" "$@" &
        sleep $_SLEEP_
    fi
}

