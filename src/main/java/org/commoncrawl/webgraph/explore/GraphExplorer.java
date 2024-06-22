/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2024 Common Crawl and contributors
 */
package org.commoncrawl.webgraph.explore;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

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
 * Utility class for graph exploration: load and hold all required web graph
 * data structures, provided methods to interactively explore the graph.
 */
public class GraphExplorer {

	private static Logger LOG = LoggerFactory.getLogger(GraphExplorer.class);

	public class Graph {
		public String name;
		public ImmutableGraph graph;
		public ImmutableGraph graphT;
		public ImmutableExternalPrefixMap vertexMap;
		public FrontCodedStringList vertexMapFcl;
		public ShiftAddXorSignedStringMap vertexMapSmph;
		public GOV4Function<String> vertexMapMph;

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
		}

		public String vertexIdToLabel(long id) {
			if (g.vertexMap != null) {
				return g.vertexMap.list().get((int) id).toString();
			} else {
				return g.vertexMapFcl.get((int) id).toString();
			}
		}

		public long vertexLabelToId(String label) {
			if (g.vertexMap != null) {
				return g.vertexMap.getLong(label);
			} else if (g.vertexMapSmph != null) {
				return g.vertexMapSmph.getLong(label);
			} else if (g.vertexMapMph != null) {
				return g.vertexMapMph.getLong(label);
			} else {
				throw new RuntimeException("No vertex map loaded.");
			}
		}
	}

	public class Vertex {
		private long id;
		private String label;

		public Vertex(String label) {
			this.label = label;
			id = g.vertexLabelToId(label);
		}

		public Vertex(long id) {
			this.id = id;
			label = g.vertexIdToLabel(id);
		}

		@Override
		public String toString() {
			return "#" + id + "\t" + label;
		}

		public int outdegree() {
			return g.graph.outdegree((int) id);
		}

		public int indegree() {
			return g.graphT.outdegree((int) id);
		}

		public int[] successors() {
			return g.graph.successorArray((int) id);
		}

		public int[] predecessors() {
			return g.graphT.successorArray((int) id);
		}
	}

	private Graph g = null;
	private Vertex v = null;

	public GraphExplorer(String name) throws Exception {
		g = new Graph(name);
	}

	public Graph getGraph() {
		return g;
	}

	public Vertex getVertex(String vertexLabel) {
		return new Vertex(vertexLabel);
	}

	public Vertex getVertex(long vertexId) {
		return new Vertex(vertexId);
	}

	public void setVertex(String vertexLabel) {
		v = getVertex(vertexLabel);
	}

	public void setVertex(long vertexId) {
		v = getVertex(vertexId);
	}

	public int outdegree() {
		return v.outdegree();
	}
	public int outdegree(long vertexId) {
		return g.graph.outdegree((int) vertexId);
	}
	public int outdegree(String vertexLabel) {
		return g.graph.outdegree((int) g.vertexLabelToId(vertexLabel));
	}
	public int indegree() {
		return v.indegree();
	}
	public int indegree(long vertexId) {
		return g.graphT.outdegree((int) vertexId);
	}
	public int indegree(String vertexLabel) {
		return g.graphT.outdegree((int) g.vertexLabelToId(vertexLabel));
	}

	public long[] sharedPredecessors(long[] vertices) {
		return sharedPredecessors(vertices, vertices.length, vertices.length);
	}

	public long[] sharedPredecessors(long[] vertices, int minShared, int maxShared) {
		return sharedSuccessors(g.graphT, vertices, minShared, maxShared);
	}

	public long[] sharedSuccessors(long[] vertices) {
		return sharedSuccessors(vertices, vertices.length, vertices.length);
	}

	public long[] sharedSuccessors(long[] vertices, int minShared, int maxShared) {
		return sharedSuccessors(g.graph, vertices, minShared, maxShared);
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
	 * @param graph     the webgraph used to achieve the successors
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

	/* Reimplementation of commands provided by pywebgraph (cn, pwn, ls, sl) */

	/**
	 * Change the current working node / vertex.
	 * 
	 * @param vertexLabel vertex label (node name)
	 */
	public void cn(String vertexLabel) {
		setVertex(vertexLabel);
		pwn();
	}

	/**
	 * Change the current working node / vertex.
	 * 
	 * @param vertexId vertex ID
	 */
	public void cn(long vertexId) {
		setVertex(vertexId);
		pwn();
	}

	/**
	 * Print the current working node / vertex.
	 */
	public void pwn() {
		if (v == null) {
			throw new NullPointerException("Current orking node not set, use cn(...) to define the working node.");
		}
		print(v.toString());
	}

	/**
	 * Print the successors (outgoing links) of the current working node / vertex.
	 */
	public void ls() {
		if (v == null) {
			throw new NullPointerException("Current orking node not set, use cn(...) to define the working node.");
		}
		ls(v.id);
	}

	/**
	 * Print the successors (outgoing links) of a vertex.
	 * 
	 * @param vertexId vertex ID
	 */
	public void ls(long vertexId) {
		printVertices(g.graph.successors((int) vertexId));
	}

	/**
	 * Print the successors (outgoing links) of a vertex.
	 * 
	 * @param vertexLabel vertex label / vertex name
	 */
	public void ls(String vertexLabel) {
		ls(g.vertexLabelToId(vertexLabel));
	}

	/**
	 * Print the predecessors (incoming links) of the current working node / vertex.
	 */
	public void sl() {
		if (v == null) {
			throw new NullPointerException("Current orking node not set, use cn(...) to define the working node.");
		}
		sl(v.id);
	}

	/**
	 * Print the predecessors (incoming links) of a vertex.
	 * 
	 * @param vertexId vertex ID
	 */
	public void sl(long vertexId) {
		printVertices(g.graphT.successors((int) vertexId));
	}

	/**
	 * Print the predecessors (incoming links) of a vertex.
	 * 
	 * @param vertexLabel vertex label / vertex name
	 */
	public void sl(String vertexLabel) {
		sl(g.vertexLabelToId(vertexLabel));
	}

	/* Utilities */

	public long[] loadVerticesFromFile(String fileName) {
		try (Stream<String> in = Files.lines(Paths.get(fileName), StandardCharsets.UTF_8)) {
			return in.mapToLong(label -> g.vertexLabelToId(label)).filter(id -> id > -1).toArray();
		} catch (IOException e) {
			LOG.error("Failed to load vertices from file {}", fileName, e);
		}
		return new long[0];
	}

	public void saveVerticesToFile(long[] vertexIDs, String fileName) {
		try (PrintStream out = new PrintStream(Files.newOutputStream(Paths.get(fileName)), false,
				StandardCharsets.UTF_8)) {
			Arrays.stream(vertexIDs).forEach(id -> out.println(g.vertexIdToLabel(id)));
		} catch (IOException e) {
			LOG.error("Failed to load vertices from file {}", fileName, e);
		}
	}

	private void print(String s) {
		System.out.println(s);
	}

	public void printVertices(LazyIntIterator it) {
		int next = it.nextInt();
		int i = 0;
		while (next != CountingMergedIntIterator.EMPTY_INPUT_ITERATOR_VALUE) {
			print(String.format("%d: %s", i, (new Vertex(next)).toString()));
			next = it.nextInt();
			i++;
		}
	}

	public void printVertices(long[] vertexIDs) {
		int i = 0;
		for (long id : vertexIDs) {
			print(String.format("%d: %s", i, (new Vertex(id)).toString()));
			i++;
		}
	}

	public void printVertices(int[] vertexIDs) {
		int i = 0;
		for (long id : vertexIDs) {
			print(String.format("%d: %s", i, (new Vertex(id)).toString()));
			i++;
		}
	}
}
