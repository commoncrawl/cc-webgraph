package org.commoncrawl.webgraph;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Stack;
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

	protected boolean countHosts = false;
	protected boolean privateDomains = false;
	protected boolean strictDomainValidate = true;

	private int[] ids;
	protected long currentId = -1;
	protected Domain lastDomain = null;
	protected long lastFromId = -1;
	protected long lastToId = -1;
	protected Stack<Domain> domainStack = new Stack<>();

	private static Pattern SPLIT_HOST_PATTERN = Pattern.compile("\\.");

	protected static class Domain {
		String name;
		String revName;
		long id;
		long numberOfHosts;
		List<Long> ids = new ArrayList<>();
		public Domain(String name, long id, long numberOfHosts) {
			this.name = name;
			this.revName = reverseHost(name);
			this.id = id;
			this.numberOfHosts = numberOfHosts;
		}
		public Domain(String name) {
			this(name, -1, 0);
		}
		public Domain(String name, long hostId) {
			this(name, -1, 0);
			add(hostId);
		}
		public void add(long hostId) {
			ids.add(hostId);
			numberOfHosts++;
		}
	}

	private HostToDomainGraph() {
	}

	public HostToDomainGraph(int maxSize) {
		ids = new int[maxSize];
	}

	public void doCount(boolean countHosts) {
		this.countHosts = countHosts;
	}

	public void doPrivateDomains(boolean privateDomains) {
		this.privateDomains = privateDomains;
	}
	public void setStrictDomainValidate(boolean strict) {
		this.strictDomainValidate = strict;
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
		String domain = EffectiveTldFinder.getAssignedDomain(host, true, !privateDomains);
		StringBuilder sb = new StringBuilder();
		if (domain == null && !strictDomainValidate) {
			if (EffectiveTldFinder.getEffectiveTLDs().containsKey(host) && host.indexOf('.') != -1) {
				LOG.info("Accepting public suffix {} (containing dot) as domain", host);
			}
			domain = host;
		}
		if (domain == null) {
			LOG.warn("No domain for host: {}", host);
			setValue(id, -1);
			return null;
		} else if (lastDomain == null) {
			// must start next domain
			lastDomain = new Domain(domain, id);
			return null;
		} else if (domain.equals(lastDomain.name)) {
			lastDomain.add(id);
			return null;
		} else if (isHyphenPrefix(lastDomain.revName, revHost)) {
			LOG.info("Found misordered domain name (containing hyphen): {} before {} in {}",
					reverseHost(lastDomain.name), reverseHost(domain), revHost);
			domainStack.push(lastDomain);
			lastDomain = new Domain(domain, id);
			return null;
		} else if (domainStack.size() > 0) {
			String revDomain = revHost.substring(0, domain.length());
			while (domainStack.size() > 0) {
				// restore lastDomain(s) from stack and assign Id now
				Domain top = domainStack.peek();
				int c = top.revName.compareTo(revDomain);
				if (c < 0 && isHyphenPrefix(top.revName, revDomain)) {
					// do not ship out top because top is a "hyphen"-prefix of domain
					c = 999;
				} else if (c > 0 && isHyphenPrefix(revDomain, top.revName)) {
					// must ship out top since current domain is a "hyphen"-prefix of top
					c = -999;
				}
				if (c <= 0 && lastDomain != null) {
					lastDomain.id = ++currentId;
					getNodeLine(sb, lastDomain);
					lastDomain = null;
				}
				if (c == 0) {
					lastDomain = domainStack.pop();
					lastDomain.add(id);
					return sb.toString();
				} else if (c < 0) {
					domainStack.pop();
					top.id = ++currentId;
					getNodeLine(sb, top);
				} else {
					break;
				}
			}
		}
		if (lastDomain != null) {
			lastDomain.id = ++currentId;
			getNodeLine(sb, lastDomain);
		}
		lastDomain = new Domain(domain, id);
		return sb.toString();
	}

	private static boolean isHyphenPrefix(String revDomainA, String revDomainB) {
		return revDomainB.length() > revDomainA.length() && revDomainB.charAt(revDomainA.length()) == '-'
				&& revDomainB.startsWith(revDomainA);
	}

	private String getNodeLine(Domain domain) {
		StringBuilder b = new StringBuilder();
		getNodeLine(b, domain);
		return b.toString();
	}

	private void getNodeLine(StringBuilder b, Domain domain) {
		if (domain == null) return;
		if (domain.id >= 0 && domain.name != null) {
			if (b.length() > 0) {
				b.append('\n');
			}
			b.append(domain.id);
			b.append('\t');
			b.append(reverseHost(domain.name));
			if (countHosts) {
				b.append('\t');
				b.append(domain.numberOfHosts);
			}
		}
		for (Long hostId : domain.ids) {
			setValue(hostId.longValue(), domain.id);
		}
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

	private void finishNodes(PrintStream out) {
		while (domainStack.size() > 0) {
			Domain domain = domainStack.pop();
			domain.id = ++currentId;
			out.println(getNodeLine(domain));
		}
		if (lastDomain != null) {
			lastDomain.id = ++currentId;
			out.println(getNodeLine(lastDomain));
		}
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

	private static void showHelp() {
		System.err.println("HostToDomainGraph [-c] <maxSize> <nodes_in> <nodes_out> <edges_in> <edges_out>");
		System.err.println("Options:");
		System.err.println(" -c\tcount hosts per domain (additional column in <nodes_out>");
		System.err.println(" --private\tconvert to private domains (from the private section of the public");
		System.err.println("          \tsuffix list, see https://publicsuffix.org/list/#list-format");
		System.err.println(" --no-strict-domain-validate\tstrictly discard potentially invalid domains");
	}

	public static void main(String[] args) {
		boolean countHosts = false;
		boolean noStrictDomainValidate = false;
		int argpos = 0;
		while (argpos < args.length && args[argpos].startsWith("-")) {
			switch (args[argpos]) {
			case "-c":
				countHosts = true;
				break;
			case "--no-strict-domain-validate":
				noStrictDomainValidate = true;
				break;
			default:
				System.err.println("Unknown option " + args[argpos]);
				showHelp();
				System.exit(1);
			}
			argpos++;
		}
		if ((args.length - argpos) < 5) {
			showHelp();
			System.exit(1);
		}
		long maxSize = 0;
		try {
			maxSize = Long.parseLong(args[argpos+0]);
		} catch (NumberFormatException e) {
			LOG.error("Invalid number: " + args[argpos+0]);
			System.exit(1);
		}
		HostToDomainGraph converter;
		if (maxSize <= Arrays.MAX_ARRAY_SIZE) {
			converter = new HostToDomainGraph((int) maxSize);
		} else {
			converter = new HostToDomainGraphBig(maxSize);
		}
		converter.doCount(countHosts);
		converter.setStrictDomainValidate(!noStrictDomainValidate);
		String nodesIn = args[argpos+1];
		String nodesOut = args[argpos+2];
		try (Stream<String> in = Files.lines(Paths.get(nodesIn));
				PrintStream out = new PrintStream(Files.newOutputStream(Paths.get(nodesOut)))) {
			converter.convert(converter::convertNode, in, out);
			converter.finishNodes(out);
			LOG.info("Finished conversion of nodes/vertices");
		} catch (IOException e) {
			LOG.error("Failed to read nodes from " + nodesIn);
			System.exit(1);
		}
		String edgesIn = args[argpos+3];
		String edgesOut = args[argpos+4];
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
