package org.commoncrawl.webgraph;


import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.floats.FloatBigArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongComparator;


/**
 * Assign ranks to harmonic centrality and page rank values, and join ranks with
 * node names.
 * 
 * Sorting and joining is done in memory. For a graph with <i>n</i> nodes, the
 * required memory is 24 * <i>n</i> bytes, resp. 36 * <i>n</i> bytes if <i>n</i>
 * &gt; {@link Arrays#MAX_ARRAY_SIZE}. In practice, the requirements are higher
 * by about 50%.
 */
public class JoinSortRanks {

	protected static Logger LOG = LoggerFactory.getLogger(JoinSortRanks.class);

	private float[] harmonicCentralityValues;
	private double[] pageRankValues;

	private int[] harmonicCentralityRanks;
	private int[] pageRankRanks;
	private int[] indirectSortPerm;

	public void loadHarmonicCentrality(String ranksHC) throws IOException {
		harmonicCentralityValues = BinIO.loadFloats(ranksHC);
		harmonicCentralityRanks = new int[harmonicCentralityValues.length];
	}

	public void loadPageRank(String ranksPR) throws IOException {
		pageRankValues = BinIO.loadDoubles(ranksPR);
		pageRankRanks = new int[pageRankValues.length];
	}

	private int compareHarmonicCentralityIndirect(int k1, int k2) {
		k1 = indirectSortPerm[k1];
		k2 = indirectSortPerm[k2];
		float f1 = harmonicCentralityValues[k1];
		float f2 = harmonicCentralityValues[k2];
		// sort in reverse order, higher values first
		if (f1 < f2) {
			return 1;
		}
		if (f1 > f2) {
			return -1;
		}
		// secondary sorting by original order (lexicographically sorted node names)
		return Integer.compare(k1, k2);
	}

	private int comparePageRankIndirect(int k1, int k2) {
		k1 = indirectSortPerm[k1];
		k2 = indirectSortPerm[k2];
		double f1 = pageRankValues[k1];
		double f2 = pageRankValues[k2];
		// sort in reverse order, higher values first
		if (f1 < f2) {
			return 1;
		}
		if (f1 > f2) {
			return -1;
		}
		// secondary sorting by original order (lexicographically sorted node names)
		return Integer.compare(k1, k2);
	}

	private void swapIndirect(int k1, int k2) {
		IntArrays.swap(indirectSortPerm, k1, k2);
	}

	private void assignRank(int[] ranks, IntComparator comp) {
		int length = ranks.length;
		indirectSortPerm = new int[length];
		for (int i = 0; i < length; i++) {
			indirectSortPerm[i] = i;
		}
		Arrays.parallelQuickSort(0, length, comp, this::swapIndirect);
		for (int i = 0; i < length; ) {
			ranks[indirectSortPerm[i]] = ++i;
		}
		indirectSortPerm = null;
	}

	public void assignHarmonicCentralityRank() {
		assignRank(harmonicCentralityRanks, this::compareHarmonicCentralityIndirect);
	}

	public void assignPageRankRank() {
		assignRank(pageRankRanks, this::comparePageRankIndirect);
	}

	protected float getHarmonicCentralityValue(long id) {
		return harmonicCentralityValues[(int) id];
	}

	protected long getHarmonicCentralityRank(long id) {
		return harmonicCentralityRanks[(int) id];
	}

	protected double getPageRankValue(long id) {
		return pageRankValues[(int) id];
	}

	protected long getPageRankRank(long id) {
		return pageRankRanks[(int) id];
	}

	public void convert(Function<String, String> func, Stream<String> in, PrintStream out) {
        in.map(func).forEach(out::println);
	}

	public String addRanks(String line) {
		int sep = line.indexOf('\t');
		if (sep == -1) {
			return "";
		}
		long id = Long.parseLong(line.substring(0, sep));
		// check whether new line is already contained
		int end = line.lastIndexOf('\n');
		String revHost = line.substring(sep+1);
		float hcv = getHarmonicCentralityValue(id);
		long hcr = getHarmonicCentralityRank(id);
		double prv = getPageRankValue(id);
		long prr = getPageRankRank(id);
		StringBuilder sb = new StringBuilder();
		sb.append(hcr);
		sb.append('\t');
		sb.append(hcv);
		sb.append('\t');
		sb.append(prr);
		sb.append('\t');
		sb.append(prv);
		sb.append('\t');
		sb.append(revHost);
		if (end != -1) {
			sb.append('\n');
		}
		return sb.toString();
	}




	public static class JoinSortRanksBig extends JoinSortRanks {

		private float[][] harmonicCentralityValues;
		private double[][] pageRankValues;

		private long[][] harmonicCentralityRanks;
		private long[][] pageRankRanks;
		private long[][] indirectSortPerm;

		public void loadHarmonicCentrality(String ranksFile) throws IOException {
			harmonicCentralityValues = BinIO.loadFloatsBig(ranksFile);
			long length = FloatBigArrays.length(harmonicCentralityValues);
			harmonicCentralityRanks = LongBigArrays.newBigArray(length);
		}

		public void loadPageRank(String ranksFile) throws IOException {
			pageRankValues = BinIO.loadDoublesBig(ranksFile);
			long length = DoubleBigArrays.length(pageRankValues);
			pageRankRanks = LongBigArrays.newBigArray(length);
		}

		private int compareHarmonicCentralityIndirect(long k1, long k2) {
			k1 = LongBigArrays.get(indirectSortPerm, k1);
			k2 = LongBigArrays.get(indirectSortPerm, k2);
			float f1 = FloatBigArrays.get(harmonicCentralityValues, k1);
			float f2 = FloatBigArrays.get(harmonicCentralityValues, k2);
			// sort in reverse order, higher values first
			if (f1 < f2) {
				return 1;
			}
			if (f1 > f2) {
				return -1;
			}
			// secondary sorting by original order (lexicographically sorted node names)
			return Long.compare(k1, k2);
		}

		private int comparePageRankIndirect(long k1, long k2) {
			k1 = LongBigArrays.get(indirectSortPerm, k1);
			k2 = LongBigArrays.get(indirectSortPerm, k2);
			double f1 = DoubleBigArrays.get(pageRankValues, k1);
			double f2 = DoubleBigArrays.get(pageRankValues, k2);
			// sort in reverse order, higher values first
			if (f1 < f2) {
				return 1;
			}
			if (f1 > f2) {
				return -1;
			}
			// secondary sorting by original order (lexicographically sorted node names)
			return Long.compare(k1, k2);
		}

		private void swapIndirect(long k1, long k2) {
			LongBigArrays.swap(indirectSortPerm, k1, k2);
		}

		private void assignRank(long[][] ranks, LongComparator comp) {
			long length = LongBigArrays.length(ranks);
			indirectSortPerm = LongBigArrays.newBigArray(length);
			for (long i = 0; i < length; i++) {
				LongBigArrays.set(indirectSortPerm, i, i);
			}
			BigArrays.quickSort(0, length, comp, this::swapIndirect);
			for (long i = 0; i < length; ) {
				LongBigArrays.set(ranks, LongBigArrays.get(indirectSortPerm, i), ++i);
			}
			indirectSortPerm = null;
		}

		public void assignHarmonicCentralityRank() {
			assignRank(harmonicCentralityRanks, this::compareHarmonicCentralityIndirect);
		}

		public void assignPageRankRank() {
			assignRank(pageRankRanks, this::comparePageRankIndirect);
		}

		protected float getHarmonicCentralityValue(long id) {
			return FloatBigArrays.get(harmonicCentralityValues, id);
		}

		protected long getHarmonicCentralityRank(long id) {
			return LongBigArrays.get(harmonicCentralityRanks, id);
		}

		protected double getPageRankValue(long id) {
			return DoubleBigArrays.get(pageRankValues, id);
		}

		protected long getPageRankRank(long id) {
			return LongBigArrays.get(pageRankRanks, id);
		}

	}

	private static void showHelp() {
		System.err.println("JoinSortRanks [--big] <node_ids_names> <hc.bin> <pr.bin> <ranks_out>");
		System.err.println("Options:");
		System.err.println(" ");
	}

	public static void main(String[] args) {
		boolean useBig = false;
		int argpos = 0;
		while (args[argpos].startsWith("-")) {
			switch (args[argpos]) {
			case "--big":
				useBig = true;
				break;
			default:
				System.err.println("Unknown option " + args[argpos]);
				showHelp();
				System.exit(1);
			}
			argpos++;
		}
		if (args.length < 4) {
			showHelp();
			System.exit(1);
		}
		JoinSortRanks converter;
		if (useBig) {
			converter = new JoinSortRanksBig();
		} else {
			converter = new JoinSortRanks();
		}

		String nodesIn = args[argpos++];
		String ranksHC = args[argpos++];
		String ranksPR = args[argpos++];
		String ranksOut = args[argpos++];
		try (Stream<String> in = Files.lines(Paths.get(nodesIn))) {
			OutputStream ranksOutStream;
			if (ranksOut.equals("-")) {
				ranksOutStream = System.out;
			} else {
				ranksOutStream = Files.newOutputStream(Paths.get(ranksOut));
			}
			PrintStream out = new PrintStream(ranksOutStream);
			LOG.info("Loading harmonic centrality values from {}", ranksHC);
			converter.loadHarmonicCentrality(ranksHC);
			LOG.info("Loading page rank values from {}", ranksPR);
			converter.loadPageRank(ranksPR);
			LOG.info("Assigning harmonic centrality ranks");
			converter.assignHarmonicCentralityRank();
			LOG.info("Assigning page rank ranks");
			converter.assignPageRankRank();
			LOG.info("Joining ranks");
			converter.convert(converter::addRanks, in, out);
			LOG.info("Finished joining ranks");
		} catch (IOException e) {
			LOG.error("Failed to join ranks:", e);
			System.exit(1);
		}
	}

}
