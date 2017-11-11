package org.commoncrawl.webgraph;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import crawlercommons.domains.EffectiveTldFinder;

/**
 * Convert host-level webgraph to domain-level webgraph. A webgraph is
 * represented by two text files/streams with tab-separated columns
 * <dl>
 * <dt>vertices</dt>
 * <dd>&langle;id, revName&rangle;</dd>
 * <dt>edges</dt>
 * <dd>&langle;fromId, toId&rangle;</dd>
 * </dl>
 * Host or domain names are reversed (<code>www.example.com</code> is written as
 * <code>com.example.www</code>). The vertices file is sorted lexicographically
 * by reversed host name, IDs (0,1,...,n) are assigned in this sort order. The
 * edges file is sorted numerically first by fromId, second by toId. These
 * sorting restrictions allow to convert large host graphs with acceptable
 * memory requirements (number of hosts &times; 4 bytes).
 */
public class HostToDomainGraph {
	
	private int[] ids;
	private int lastId = -1;
	private int lastFromId = -1;
	private int lastToId = -1;
	String lastDomain = "";
	
	private static Pattern SPLIT_HOST_PATTERN = Pattern.compile("\\.");
	
	public HostToDomainGraph(int maxSize) {
		ids = new int[maxSize];
	}

	public static String reverseHost(String revHost) {
		String[] rev = SPLIT_HOST_PATTERN.split(revHost);
		for (int i = 0; i < (rev.length/2); i++) {
		    String temp = rev[i];
		    rev[i] = rev[rev.length - i - 1];
		    rev[rev.length - i - 1] = temp;
		}
		return String.join(".", rev);
	}

	public String convertNode(String line) {
		int sep = line.indexOf('\t');
		if (sep == -1) {
			return "";
		}
		int id = Integer.parseInt(line.substring(0, sep));
		String revHost = line.substring(sep+1);
		String host = reverseHost(revHost);
		String domain = EffectiveTldFinder.getAssignedDomain(host, true);
		if (domain == null) {
			ids[id] = -1;
			return null;
		} else if (domain.equals(lastDomain)) {
			ids[id] = lastId;
			return null;
		}
		lastId++;
		ids[id] = lastId;
		lastDomain = domain;
		return lastId + "\t" + reverseHost(domain);
	}
	
	public String convertEdge(String line) {
		int sep = line.indexOf('\t');
		if (sep == -1) {
			return "";
		}
		int fromId = Integer.parseInt(line.substring(0, sep));
		int toId = Integer.parseInt(line.substring(sep+1));
		fromId = ids[fromId];
		toId = ids[toId];
		if (fromId == toId || fromId == -1 || toId == -1
				|| (lastFromId == fromId && lastToId == toId)) {
			return null;
		}
		lastFromId = fromId;
		lastToId = toId;
		return fromId + "\t" + toId;
	}

	public void convert(Function<String, String> func, Stream<String> in, PrintStream out) {
        in.map(func).filter(Objects::nonNull).forEach(out::println);
	}

	public static void main(String[] args) {
		if (args.length != 5) {
			System.err.println("HostToDomainGraph <maxSize> <nodes_in> <edges_in> <nodes_out> <edges_out>");
			System.exit(1);
		}
		int maxSize = 0;
		try {
			maxSize = Integer.parseInt(args[0]); 
		} catch (NumberFormatException e) {
			System.err.println("Invalid number: " + args[0]);
			System.exit(1);
		}
		HostToDomainGraph converter = new HostToDomainGraph(maxSize);
		String nodesIn = args[1];
		String nodesOut = args[2];
		try (Stream<String> in = Files.lines(Paths.get(nodesIn));
				PrintStream out = new PrintStream(Files.newOutputStream(Paths.get(nodesOut)))) {
			converter.convert(converter::convertNode, in, out);
		} catch (IOException e) {
			System.err.println("Failed to read nodes from " + nodesIn);
			System.exit(1);
		}
		String edgesIn = args[3];
		String edgesOut = args[4];
		try (Stream<String> in = Files.lines(Paths.get(edgesIn));
				PrintStream out = new PrintStream(Files.newOutputStream(Paths.get(edgesOut)))) {
			converter.convert(converter::convertEdge, in, out);
		} catch (IOException e) {
			System.err.println("Failed to read edges from " + edgesIn);
			System.exit(1);
		}
	}
}
