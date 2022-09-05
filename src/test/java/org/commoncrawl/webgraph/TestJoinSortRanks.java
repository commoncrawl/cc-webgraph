/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2022 Common Crawl and contributors
 */
package org.commoncrawl.webgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;

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
            BinIO.storeDoubles(arr, file);
            LOG.info("Stored {} doubles in file {} of size {}", arrSize, file.getAbsolutePath(), file.length());
            arr = BinIO.loadDoubles(file.getAbsolutePath());
            assertEquals(arrSize, arr.length);
            assertEquals(intOverflow, file.length());
        } catch (IOException e) {
            fail("Failed to store and load double array: " + e);
        } finally {
            file.delete();
        }
    }
}
