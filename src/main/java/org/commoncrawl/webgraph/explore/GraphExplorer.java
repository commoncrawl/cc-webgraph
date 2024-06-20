/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2024 Common Crawl and contributors
 */
package org.commoncrawl.webgraph.explore;

import java.io.IOException;

import org.commoncrawl.webgraph.CountingMergedIntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
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

		public Graph(String name) {
			this.name = name;
			try {
				graph = ImmutableGraph.loadMapped(name);
				graphT = ImmutableGraph.loadMapped(name + "-t");
				vertexMap = (ImmutableExternalPrefixMap) BinIO.loadObject(name + ".iepm");
			} catch (IOException | ClassNotFoundException e) {
				LOG.error("Failed to load graph {}:", name, e);
			}
			// TODO: fall-back: instead of .iepm load .mph .fcl .smph
		}
	}

	public class Vertex {
		private long id;
		private String label;

		public Vertex(String label) {
			this.label = label;
			id = g.vertexMap.getLong(label);
		}

		public Vertex(long id) {
			this.id = id;
			label = g.vertexMap.list().get((int) id).toString();
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

	public GraphExplorer(String name) {
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
		return g.graph.outdegree((int) g.vertexMap.getLong(vertexLabel));
	}
	public int indegree() {
		return v.indegree();
	}
	public int indegree(long vertexId) {
		return g.graphT.outdegree((int) vertexId);
	}
	public int indegree(String vertexLabel) {
		return g.graphT.outdegree((int) g.vertexMap.getLong(vertexLabel));
	}

	public long[] commonPredecessors(long[] vertices) {
		return commonPredecessors(vertices, vertices.length);
	}

	public long[] commonPredecessors(long[] vertices, int minCommon) {
		return commonSuccessors(g.graphT, vertices, minCommon);
	}

	public long[] commonSuccessors(long[] vertices) {
		return commonSuccessors(vertices, vertices.length);
	}

	public long[] commonSuccessors(long[] vertices, int minCommon) {
		return commonSuccessors(g.graph, vertices, minCommon);
	}

	public long[] commonSuccessors(ImmutableGraph graph, long[] vertices, int minCommon) {
		LazyIntIterator[] iters = new LazyIntIterator[vertices.length];
		for (int i = 0; i < vertices.length; i++) {
			iters[i] = graph.successors((int) vertices[i]);
		}
		CountingMergedIntIterator iter = new CountingMergedIntIterator(iters);
		LongArrayList res = new LongArrayList();
		int id;
		while (iter.hasNext()) {
			id = iter.nextInt();
			if (iter.getCount() >= minCommon) {
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
		ls(g.vertexMap.getLong(vertexLabel));
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
		sl(g.vertexMap.getLong(vertexLabel));
	}

	/* Utilities */

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
