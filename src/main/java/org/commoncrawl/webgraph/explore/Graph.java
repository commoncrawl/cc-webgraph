/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2024 Common Crawl and contributors
 */
package org.commoncrawl.webgraph.explore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.commoncrawl.webgraph.CountingMergedIntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.sux4j.mph.GOV4Function;
import it.unimi.dsi.util.FrontCodedStringList;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

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
		} else {
			return vertexMapFcl.get((int) id).toString();
		}
	}

	public long vertexLabelToId(String label) {
		if (vertexMap != null) {
			return vertexMap.getLong(label);
		} else if (vertexMapSmph != null) {
			return vertexMapSmph.getLong(label);
		} else if (vertexMapMph != null) {
			return vertexMapMph.getLong(label);
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
}
