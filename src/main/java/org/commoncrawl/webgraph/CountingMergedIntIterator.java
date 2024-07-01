/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2024 Common Crawl and contributors
 */
package org.commoncrawl.webgraph;

import java.util.PriorityQueue;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;

/**
 * An iterator counting the integers returned by multiple
 * {@link LazyIntIterator}s. The input iterators must return integers in a
 * monotonically non-decreasing order. The resulting iterator returns the
 * unified input integers in strictly non-decreasing order. The method
 * {@link getCount()} is used to access the count of the integer returned last
 * by {@link nextInt()}. The count equals the number of times any of the
 * iterators returned the current integer value. See also
 * {@link it.unimi.dsi.webgraph.MergedIntIterator}.
 */
public class CountingMergedIntIterator implements IntIterator {

	protected class QueuedIterator implements Comparable<QueuedIterator> {
		LazyIntIterator iter;
		int value;

		public QueuedIterator(LazyIntIterator iterator) {
			iter = iterator;
			value = iterator.nextInt();
		}

		@Override
		public int compareTo(QueuedIterator o) {
			if (value < o.value) {
				return -1;
			}
			if (value > o.value) {
				return 1;
			}
			return 0;
		}
	}

	public static int LAZY_INT_ITERATOR_EMPTY_VALUE = LazyIntIterators.EMPTY_ITERATOR.nextInt();

	private final PriorityQueue<QueuedIterator> iters = new PriorityQueue<>();
	private int currentCount = 0;

	/**
	 * @param iterators input iterators
	 */
	public CountingMergedIntIterator(LazyIntIterator... iterators) {
		for (final LazyIntIterator iter : iterators) {
			final QueuedIterator qiter = new QueuedIterator(iter);
			if (qiter.value != LAZY_INT_ITERATOR_EMPTY_VALUE) {
				iters.add(qiter);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		return iters.size() > 0;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @deprecated Please use {@link nextInt()} instead.
	 */
	@Deprecated
	@Override
	public Integer next() {
		return Integer.valueOf(nextInt());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int nextInt() {
		QueuedIterator qiter = iters.peek();
		final int value = qiter.value;
		int count = 1;
		while (true) {
			iters.remove();
			int val;
			while ((val = qiter.iter.nextInt()) == value) {
				count++;
			}
			if (val != LAZY_INT_ITERATOR_EMPTY_VALUE) {
				qiter.value = val;
				iters.add(qiter);
			}
			if (iters.isEmpty()) {
				break;
			}
			qiter = iters.peek();
			if (qiter.value == value) {
				count++;
			} else {
				break;
			}
		}
		currentCount = count;
		return value;
	}

	/**
	 * @return the count how often the last integer (returned by {@link nextInt()})
	 *         was seen in the input iterators
	 */
	public int getCount() {
		return currentCount;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int skip(int n) {
		int i = 0;
		while (i < n && hasNext()) {
			nextInt();
			i++;
		}
		return i;
	}

}
