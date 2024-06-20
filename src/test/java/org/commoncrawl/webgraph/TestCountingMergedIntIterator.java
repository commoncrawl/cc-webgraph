/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2022 Common Crawl and contributors
 */
package org.commoncrawl.webgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;

public class TestCountingMergedIntIterator {

	protected static Logger LOG = LoggerFactory.getLogger(TestCountingMergedIntIterator.class);

	@Test
	void testSimple() {
		CountingMergedIntIterator iter = new CountingMergedIntIterator(LazyIntIterators.EMPTY_ITERATOR);
		assertFalse(iter.hasNext());

		int[][][]  testArrays = { //
				{{0, 1}}, //
				{{0}, {1}}, //
				{{1}, {0}}, //
				{{1}, {0}, {}}, //
				{{1}, {0}, {}, {0}, {0}}, //
				{{1}, {0}, {}, {0}, {0, 1}}, //
				// tests for input arrays with repeating numbers
				{{1, 1}, {0, 0}, {}, {0, 0}, {0, 0}}, //
				{{1, 1}, {0, 0}, {}, {0}, {0, 1}} //
		};

		for (int[][] tArrays : testArrays) {
			LazyIntIterator[] tIters = new LazyIntIterator[tArrays.length];
			int totalCountExpected = 0;
			for (int i = 0; i < tArrays.length; i++) {
				tIters[i] = LazyIntIterators.wrap(tArrays[i]);
				totalCountExpected += tArrays[i].length;
			}
			int totalCount = 0;
			iter = new CountingMergedIntIterator(tIters);
			assertTrue(iter.hasNext());
			
			assertEquals(0, iter.nextInt());
			assertTrue(iter.getCount() > 0);
			totalCount += iter.getCount();
			assertTrue(iter.hasNext());
			assertEquals(1, iter.nextInt());
			assertTrue(iter.getCount() > 0);
			totalCount += iter.getCount();
			assertFalse(iter.hasNext());
			assertEquals(totalCountExpected, totalCount,
					"expected total count for input " + Arrays.deepToString(tArrays) + " is " + totalCountExpected);
		}

		// test skip(n)
		for (int n = 0; n <= 5; n++) {
			for (int[][] tArrays : testArrays) {
				LazyIntIterator[] tIters = new LazyIntIterator[tArrays.length];
				for (int i = 0; i < tArrays.length; i++) {
					tIters[i] = LazyIntIterators.wrap(tArrays[i]);
				}
				iter = new CountingMergedIntIterator(tIters);
				assertEquals(Math.min(n, 2), iter.skip(n));
			}
		}
	}

}
