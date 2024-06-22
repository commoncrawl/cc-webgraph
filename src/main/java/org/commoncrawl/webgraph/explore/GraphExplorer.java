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

import it.unimi.dsi.webgraph.LazyIntIterator;

/**
 * Utility class for graph exploration: load and hold all required web graph
 * data structures, provided methods to interactively explore the graph.
 */
public class GraphExplorer {

	private static Logger LOG = LoggerFactory.getLogger(GraphExplorer.class);

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
