/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2024 Common Crawl and contributors
 */
package org.commoncrawl.webgraph.explore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.commoncrawl.webgraph.CountingMergedIntIterator;
import org.commoncrawl.webgraph.HostToDomainGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.domains.EffectiveTldFinder;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOV4Function;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.LiterallySignedStringMap;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.LazyIntIterators;

/**
 * Holds webgraph-related data structures and access methods for graph
 * exploration.
 */
public class Graph {

	private static Logger LOG = LoggerFactory.getLogger(Graph.class);

	/** The base name of the graph */
	public String name;
	/** The graph */
	public ImmutableGraph graph;
	/** The transpose of the graph */
	public ImmutableGraph graphT;

	/* Maps to translate between vertex label an ID */
	protected ImmutableExternalPrefixMap vertexMap;
	protected FrontCodedStringList vertexMapFcl;
	protected ShiftAddXorSignedStringMap vertexMapSmph;
	protected GOV4Function<String> vertexMapMph;
	protected LiterallySignedStringMap vertexMapLmap;

	private static int LAZY_INT_ITERATOR_EMPTY_VALUE = LazyIntIterators.EMPTY_ITERATOR.nextInt();

	public Graph(String name) throws Exception {
		this.name = name;
		try {
			LOG.info("Loading graph {}.graph", name);
			graph = ImmutableGraph.loadMapped(name);
			LOG.info("Loading transpose of the graph {}-t.graph", name);
			graphT = ImmutableGraph.loadMapped(name + "-t");
			if (Files.exists(Paths.get(name + ".iepm"))) {
				LOG.info("Loading vertex map {}.iepm (ImmutableExternalPrefixMap)", name);
				vertexMap = (ImmutableExternalPrefixMap) BinIO.loadObject(name + ".iepm");
			} else if (Files.exists(Paths.get(name + ".fcl"))) {
				LOG.info("Loading vertex map {}.fcl (FrontCodedStringList, maps vertex IDs to labels)", name);
				vertexMapFcl = (FrontCodedStringList) BinIO.loadObject(name + ".fcl");
				if (Files.exists(Paths.get(name + ".smph"))) {
					LOG.info("Loading vertex map {}.smph (string map perfect hash, maps vertex labels to IDs)", name);
					vertexMapSmph = (ShiftAddXorSignedStringMap) BinIO.loadObject(name + ".smph");
				} else if (Files.exists(Paths.get(name + ".mph"))) {
					LOG.info("Loading vertex map {}.mph (minimal perfect hash, maps vertex labels to IDs)", name);
					vertexMapMph = (GOV4Function<String>) BinIO.loadObject(name + ".mph");
					LOG.warn(
							"Using a minimal perfect hash as vertex map does not allow to verify that a vertex label exists. "
									+ "Non-existant labels are mapped to quasi-random IDs.");
				} else {
					LOG.error("No vertex mapping found, cannot translate from vertex names to IDs.");
				}
			} else if (Files.exists(Paths.get(name + ".lmap"))) {
				LOG.info("Loading vertex map {}.lmap (LiterallySignedStringMap)", name);
				vertexMapLmap = (LiterallySignedStringMap) BinIO.loadObject(name + ".lmap");
			} else {
				LOG.error("No vertex mapping found, cannot translate from vertex names to IDs.");
			}
		} catch (IOException | ClassNotFoundException e) {
			LOG.error("Failed to load graph {}:", name, e);
			throw e;
		}
		LOG.info("Loaded graph {}.graph", name);
	}

	public String vertexIdToLabel(long id) {
		if (vertexMap != null) {
			return vertexMap.list().get((int) id).toString();
		} else if (vertexMapFcl != null) {
			return vertexMapFcl.get((int) id).toString();
		} else if (vertexMapLmap != null) {
			return vertexMapLmap.list().get((int) id).toString();
		} else {
			throw new RuntimeException("No vertex map loaded.");
		}
	}

	public long vertexLabelToId(String label) {
		if (vertexMap != null) {
			return vertexMap.getLong(label);
		} else if (vertexMapSmph != null) {
			return vertexMapSmph.getLong(label);
		} else if (vertexMapMph != null) {
			return vertexMapMph.getLong(label);
		} else if (vertexMapLmap != null) {
			return vertexMapLmap.getLong(label);
		} else {
			throw new RuntimeException("No vertex map loaded.");
		}
	}

	public int outdegree(long vertexId) {
		return graph.outdegree((int) vertexId);
	}

	public int outdegree(String vertexLabel) {
		return graph.outdegree((int) vertexLabelToId(vertexLabel));
	}

	public int indegree(long vertexId) {
		return graphT.outdegree((int) vertexId);
	}

	public int indegree(String vertexLabel) {
		return graphT.outdegree((int) vertexLabelToId(vertexLabel));
	}

	public int[] successors(long vertexId) {
		return graph.successorArray((int) vertexId);
	}

	public int[] successors(String vertexLabel) {
		return graph.successorArray((int) vertexLabelToId(vertexLabel));
	}

	public Stream<String> successorStream(String vertexLabel) {
		return successorStream(graph, vertexLabelToId(vertexLabel));
	}

	public IntStream successorIntStream(String vertexLabel) {
		return successorIntStream(graph, vertexLabelToId(vertexLabel));
	}

	public Stream<String> successorStream(String vertexLabel, String prefix) {
		return successorStream(graph, vertexLabelToId(vertexLabel), vertexMap.getInterval(prefix));
	}

	public IntStream successorIntStream(String vertexLabel, String prefix) {
		return successorIntStream(graph, vertexLabelToId(vertexLabel), vertexMap.getInterval(prefix));
	}

	public Stream<Entry<String, Long>> successorTopLevelDomainCounts(String vertexLabel) {
		return successorTopLevelDomainCounts(graph, vertexLabelToId(vertexLabel));
	}

	public Stream<String> successorStream(ImmutableGraph graph, long vertexId) {
		return successorIntStream(graph, vertexId).mapToObj(i -> vertexIdToLabel(i));
	}

	public IntStream successorIntStream(ImmutableGraph graph, long vertexId) {
		return Arrays.stream(graph.successorArray((int) vertexId));
	}

	private Stream<String> successorStream(ImmutableGraph graph, long vertexId, Interval interval) {
		return successorIntStream(graph, vertexId, interval).mapToObj(i -> vertexIdToLabel(i));
	}

	public IntStream successorIntStream(ImmutableGraph graph, long vertexId, Interval interval) {
		return Arrays.stream(graph.successorArray((int) vertexId)).filter(x -> (interval.compareTo(x) == 0));
	}

	public Stream<String> successorTopLevelDomainStream(ImmutableGraph graph, long vertexId) {
		return Arrays.stream(graph.successorArray((int) vertexId)).mapToObj(i -> getTopLevelDomain(vertexIdToLabel(i)));
	}

	public Stream<Entry<String, Long>> successorTopLevelDomainCounts(ImmutableGraph graph, long vertexId) {
		if (vertexMap != null) {
			/*
			 * speed up if we have a prefix map, utilizing the fact that vertex labels are
			 * lexicographically sorted by reversed domain name
			 */
			List<Entry<String, Long>> res = new LinkedList<>();
			LazyIntIterator iter = graph.successors((int) vertexId);
			int curr = iter.nextInt();
			while (curr != LAZY_INT_ITERATOR_EMPTY_VALUE) {
				final MutableString currLabel = vertexMap.list().get(curr);
				final int pos = currLabel.indexOf('.');
				final MutableString tldPrefix;
				final String tld;
				if (pos > -1 && (pos + 1) < currLabel.length()) {
					tldPrefix = currLabel.substring(0, pos + 1);
					tld = tldPrefix.substring(0, pos).toString();
				} else {
					tldPrefix = currLabel;
					tld = currLabel.toString();
				}
				long count = 1;
				final Interval interval = vertexMap.getInterval(tldPrefix);
				int next;
				while ((next = iter.nextInt()) != LAZY_INT_ITERATOR_EMPTY_VALUE) {
					if (next > interval.right) {
						break;
					}
					count++;
				}
				curr = next;
				res.add(new SimpleEntry<>(tld, count));
			}
			return res.stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		}
		return GraphExplorer.frequencies(successorTopLevelDomainStream(graph, vertexId));
	}

	public Stream<Entry<String, Long>> topLevelDomainCounts(IntStream vertexIds) {
		if (vertexMap != null) {
			List<Entry<String, Long>> res = new LinkedList<>();
			PrimitiveIterator.OfInt iter = vertexIds.iterator();
			if (iter.hasNext()) {
				int curr = iter.nextInt();
				do {
					final MutableString currLabel = vertexMap.list().get(curr);
					final int pos = currLabel.indexOf('.');
					final MutableString tldPrefix;
					final String tld;
					if (pos > -1 && (pos + 1) < currLabel.length()) {
						tldPrefix = currLabel.substring(0, pos + 1);
						tld = tldPrefix.substring(0, pos).toString();
					} else {
						tldPrefix = currLabel;
						tld = currLabel.toString();
					}
					long count = 1;
					final Interval interval = vertexMap.getInterval(tldPrefix);
					int next = -1;
					while (iter.hasNext()) {
						next = iter.nextInt();
						if (next > interval.right) {
							break;
						}
						count++;
					}
					res.add(new SimpleEntry<>(tld, count));
					curr = next;
					if (!iter.hasNext()) {
						break;
					}
				} while (curr > -1);
			}
			return res.stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));
		}
		return GraphExplorer.frequencies(vertexIds.mapToObj(i -> Graph.getTopLevelDomain(vertexIdToLabel(i))));
	}

	public int[] predecessors(long vertexId) {
		return graphT.successorArray((int) vertexId);
	}

	public int[] predecessors(String vertexLabel) {
		return graphT.successorArray((int) vertexLabelToId(vertexLabel));
	}

	public Stream<String> predecessorStream(String vertexLabel) {
		return successorStream(graphT, vertexLabelToId(vertexLabel));
	}

	public IntStream predecessorIntStream(String vertexLabel) {
		return successorIntStream(graphT, vertexLabelToId(vertexLabel));
	}

	public Stream<String> predecessorStream(String vertexLabel, String prefix) {
		return successorStream(graphT, vertexLabelToId(vertexLabel), vertexMap.getInterval(prefix));
	}

	public IntStream predecessorIntStream(String vertexLabel, String prefix) {
		return successorIntStream(graphT, vertexLabelToId(vertexLabel), vertexMap.getInterval(prefix));
	}

	public Stream<Entry<String, Long>> predecessorTopLevelDomainCounts(String vertexLabel) {
		return successorTopLevelDomainCounts(graphT, vertexLabelToId(vertexLabel));
	}

	public long[] sharedPredecessors(long[] vertices) {
		return sharedPredecessors(vertices, vertices.length, vertices.length);
	}

	public long[] sharedPredecessors(long[] vertices, int minShared, int maxShared) {
		return sharedSuccessors(graphT, vertices, minShared, maxShared);
	}

	public long[] sharedSuccessors(long[] vertices) {
		return sharedSuccessors(vertices, vertices.length, vertices.length);
	}

	public long[] sharedSuccessors(long[] vertices, int minShared, int maxShared) {
		return sharedSuccessors(graph, vertices, minShared, maxShared);
	}

	/**
	 * Get shared successors (children) of all {@code vertices} in a {@code graph}.
	 * The parameters {@code minShared} and {@code maxShared} allow to select the
	 * intersection, the union or a subset with a specific overlap (shared
	 * successors). If vertex <i>a</i> has the successors <i>d, e</i>, vertex
	 * <i>b</i> has <i>d, f</i> and vertex <i>c</i> has <i>d, e, g</i>, then
	 * <ul>
	 * <li>{@code minShared} = {@code maxShared} = {@code vertices.length} returns
	 * the intersection (<i>d</i>)</li>
	 * <li>{@code minShared} = 1 and {@code maxShared} = {@code vertices.length}
	 * returns the union (<i>d, e, f</i>)</li>
	 * <li>{@code minShared} = {@code maxShared} = 2 returns all successors shared
	 * by exactly two of the {@code vertices} (<i>e</i>)</li>
	 * </ul>
	 * 
	 * @param graph     the graph used to access the successors of a vertex (the
	 *                  transpose of the graph will give the predecessors of the
	 *                  vertex)
	 * @param vertices  list of vertex IDs
	 * @param minShared the minimum number of shared links to successors
	 * @param maxShared the minimum number of shared links to successors
	 * @return shared successors
	 */
	public long[] sharedSuccessors(ImmutableGraph graph, long[] vertices, int minShared, int maxShared) {
		LazyIntIterator[] iters = new LazyIntIterator[vertices.length];
		for (int i = 0; i < vertices.length; i++) {
			iters[i] = graph.successors((int) vertices[i]);
		}
		CountingMergedIntIterator iter = new CountingMergedIntIterator(iters);
		LongArrayList res = new LongArrayList();
		int id;
		while (iter.hasNext()) {
			id = iter.nextInt();
			if (iter.getCount() >= minShared && iter.getCount() <= maxShared) {
				res.add(id);
			}
		}
		res.trim();
		return res.elements();
	}

	public static String getTopLevelDomain(String reversedDomainName) {
		int dot = reversedDomainName.indexOf('.');
		if (dot < reversedDomainName.length()) {
			return reversedDomainName.substring(0, dot);
		}
		return reversedDomainName;
	}

	/**
	 * Get the registered domain for a host name based on the ICANN section of the
	 * <a href="https://www.publicsuffix.org/">public suffix list</a>.
	 * 
	 * @see EffectiveTldFinder
	 * 
	 * @param hostName host name, e.g. <code>www.example.org.uk</code>
	 * @param strict   if true return null instead of <code>hostName</code> if no
	 *                 valid public suffix is detected
	 * @return the domain name below the public suffix, e.g.
	 *         <code>example.org.uk</code>
	 */
	public static String getRegisteredDomain(String hostName, boolean strict) {
		return EffectiveTldFinder.getAssignedDomain(hostName, strict, true);
	}

	/**
	 * Get the registered domain for a host name, both in
	 * <a href= "https://en.wikipedia.org/wiki/Reverse_domain_name_notation">reverse
	 * domain name notation</a>.
	 * 
	 * @see #getRegisteredDomain(String, boolean)
	 * 
	 * @param reversedHostName host name in reverse domain name notation, e.g.
	 *                         <code>uk.ork.example.www</code>
	 * @param strict           if true return null instead of
	 *                         <code>reversedHostName</code> if no valid public
	 *                         suffix is detected
	 * @return the domain name below the public suffix, e.g.
	 *         <code>uk.org.example</code> (in reverse domain name notation)
	 */
	public static String getRegisteredDomainReversed(String reversedHostName, boolean strict) {
		String hostName = reverseDomainName(reversedHostName);
		String domainName = getRegisteredDomain(hostName, strict);
		if (strict && domainName == null) {
			return null;
		} else if (hostName.equals(domainName)) {
			return reversedHostName;
		}
		return reverseDomainName(domainName);
	}

	/**
	 * Reverse or "unreverse" a host/domain name: <code>com.example.www</code> is
	 * reversed to <code>www.example.com</code> and vice versa.
	 * 
	 * @param domainName domain name
	 * @return domain name with <a href=
	 *         "https://en.wikipedia.org/wiki/Reverse_domain_name_notation">reverse
	 *         domain name notation</a> (un)applied
	 */
	public static String reverseDomainName(String domainName) {
		return HostToDomainGraph.reverseHost(domainName);
	}
}
