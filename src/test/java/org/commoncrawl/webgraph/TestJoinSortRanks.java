/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2022 Common Crawl and contributors
 */
package org.commoncrawl.webgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.io.BinIO;

public class TestJoinSortRanks {

    protected static Logger LOG = LoggerFactory.getLogger(TestJoinSortRanks.class);

    /**
     * Reproduce issue in fastutil 8.5.8 loading (double) arrays from files of
     * size 2^31 bytes or more.
     */
    @Disabled("Fixed in fastutil 8.5.9")
    @Test
    void testLoadingDoubleArray() {
        File file;
        try {
            file = File.createTempFile("test", ".bin");
        } catch (IOException e) {
            LOG.error("Skipping test, failed to create temporary file to hold array:", e);
            return;
        }
        long intOverflow = 1L << 31;
        int arrSize = (int) (intOverflow / Double.BYTES);
        double[] arr = new double[arrSize];
        try {
            LOG.info("Storing double array of length {} in file {}", arrSize, file.getAbsolutePath());
            BinIO.storeDoubles(arr, file);
            LOG.info("Successfully stored double array of length {} in file {}, resulting file size: {} bytes", arrSize, file.getAbsolutePath(), file.length());
            assertEquals(intOverflow, file.length());
            LOG.info("Trying to clean up Java heap space...");
            arr = null;
            System.gc();
            LOG.info("Loading double array from file {}", file.getAbsolutePath());
            arr = BinIO.loadDoubles(file.getAbsolutePath());
            assertEquals(arrSize, arr.length);
            LOG.info("Successfully loaded double array of length {} from file {}", arr.length, file.getAbsolutePath());
        } catch (IOException e) {
            fail("Failed to store and load double array: " + e);
        } finally {
            file.delete();
        }
    }
}
