package org.commoncrawl.webgraph;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.domains.EffectiveTldFinder;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

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

	protected static Logger LOG = LoggerFactory.getLogger(HostToDomainGraph.class);

	private int[] ids;
	protected long lastId = -1;
	protected long lastFromId = -1;
	protected long lastToId = -1;
	protected String lastDomain = "";

	private static Pattern SPLIT_HOST_PATTERN = Pattern.compile("\\.");

	private HostToDomainGraph() {
	}

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

	protected void setValue(long id, long value) {
		ids[(int) id] = (int) value;
	}

	protected long getValue(long id) {
		return ids[(int) id];
	}

	public String convertNode(String line) {
		int sep = line.indexOf('\t');
		if (sep == -1) {
			return "";
		}
		long id = Long.parseLong(line.substring(0, sep));
		String revHost = line.substring(sep+1);
		String host = reverseHost(revHost);
		String domain = EffectiveTldFinder.getAssignedDomain(host, true, true);
		if (domain == null) {
			setValue(id, -1);
			return null;
		} else if (domain.equals(lastDomain)) {
			setValue(id, lastId);
			return null;
		}
		lastId++;
		setValue(id, lastId);
		lastDomain = domain;
		return lastId + "\t" + reverseHost(domain);
	}

	public String convertEdge(String line) {
		int sep = line.indexOf('\t');
		if (sep == -1) {
			return "";
		}
		long fromId = Long.parseLong(line.substring(0, sep));
		long toId = Long.parseLong(line.substring(sep+1));
		fromId = getValue(fromId);
		toId = getValue(toId);
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

	public static class HostToDomainGraphBig extends HostToDomainGraph {

		private long[][] ids;

		public HostToDomainGraphBig(long maxSize) {
			ids = LongBigArrays.newBigArray(maxSize);
		}

		protected void setValue(long id, long value) {
			LongBigArrays.set(ids, id, value);
		}

		protected long getValue(long id) {
			return LongBigArrays.get(ids, id);
		}
	}

	public static void main(String[] args) {
		if (args.length != 5) {
			LOG.error("HostToDomainGraph <maxSize> <nodes_in> <nodes_out> <edges_in> <edges_out>");
			System.exit(1);
		}
		long maxSize = 0;
		try {
			maxSize = Long.parseLong(args[0]);
		} catch (NumberFormatException e) {
			LOG.error("Invalid number: " + args[0]);
			System.exit(1);
		}
		HostToDomainGraph converter;
		if (maxSize <= Arrays.MAX_ARRAY_SIZE) {
			converter = new HostToDomainGraph((int) maxSize);
		} else {
			converter = new HostToDomainGraphBig(maxSize);
		}
		String nodesIn = args[1];
		String nodesOut = args[2];
		try (Stream<String> in = Files.lines(Paths.get(nodesIn));
				PrintStream out = new PrintStream(Files.newOutputStream(Paths.get(nodesOut)))) {
			converter.convert(converter::convertNode, in, out);
			LOG.info("Finished conversion of nodes/vertices");
		} catch (IOException e) {
			LOG.error("Failed to read nodes from " + nodesIn);
			System.exit(1);
		}
		String edgesIn = args[3];
		String edgesOut = args[4];
		try (Stream<String> in = Files.lines(Paths.get(edgesIn));
				PrintStream out = new PrintStream(Files.newOutputStream(Paths.get(edgesOut)))) {
			converter.convert(converter::convertEdge, in, out);
			LOG.info("Finished conversion of edges");
		} catch (IOException e) {
			LOG.error("Failed to read edges from " + edgesIn);
			System.exit(1);
		}
	}
}
