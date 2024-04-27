/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2022 Common Crawl and contributors
 */
package org.commoncrawl.webgraph.explore;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

import java.io.IOException;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;

public class GraphExplorer {

	public class Graph {
		String name;
		ImmutableGraph graph;
		ImmutableGraph graphT;
		ImmutableExternalPrefixMap vertexMap;

		public Graph(String name) {
			this.name = name;
			try {
				graph = ImmutableGraph.loadMapped(name);
				graphT = ImmutableGraph.loadMapped(name + "-t");
				vertexMap = (ImmutableExternalPrefixMap) BinIO.loadObject(name + ".iepm");
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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

	public void setVertex(String vertexLabel) {
		v = getVertex(vertexLabel);
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

	/* reimplementation of commands provided by pywebgraph (cn, pwn, ls, sl) */
	public void cn(String nodeLabel) {
		setVertex(nodeLabel);
		pwn();
	}

	public void pwn() {
		print(v.toString());
	}

	public void ls() {
		ls(v.id);
	}
	public void ls(long vertexId) {
		printVertices(g.graph.successors((int) vertexId));
	}
	public void ls(String vertexLabel) {
		ls(g.vertexMap.getLong(vertexLabel));
	}

	public void sl() {
		sl(v.id);
	}
	public void sl(long vertexId) {
		printVertices(g.graphT.successors((int) vertexId));
	}
	public void sl(String vertexLabel) {
		sl(g.vertexMap.getLong(vertexLabel));
	}

	/* utilities */
	private void print(String s) {
		System.out.println(s);
	}
	private void print(int s) {
		System.out.println(s);
	}

	private void printVertices(LazyIntIterator it) {
		int next = it.nextInt();
		int i = 0;
		while (next != -1) {
			print(String.format("%d: %s", i, (new Vertex(next)).toString()));
			next = it.nextInt();
			i++;
		}
	}
}
