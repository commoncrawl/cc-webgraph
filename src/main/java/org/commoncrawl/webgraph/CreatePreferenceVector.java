package org.commoncrawl.webgraph;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;


/**
 * Create a preference vector used for PageRank calculations, e.g.,
 * (Anti)TrustRank. See <a href=
 * "http://law.di.unimi.it/software/law-docs/it/unimi/dsi/law/rank/PageRank.html#buildProperties-java.lang.String-java.lang.String-java.lang.String-">PageRank.buildProperties(...)</a>.
 */
public class CreatePreferenceVector {

	protected static Logger LOG = LoggerFactory.getLogger(CreatePreferenceVector.class);

	private long lastId = 0;
	private Iterator<String> preferenceIterator;
	private LongList preferenceIds = new LongArrayList();
	private double defaultPreferenceValue;
	private String nextPreferenceName;
	long recordsProcessed;
	long preferenceNamesFound;


	public CreatePreferenceVector(double defVal) {
		defaultPreferenceValue = defVal;
	}

	private boolean nextPreferenceElement() {
		if (preferenceIterator.hasNext()) {
			nextPreferenceName = preferenceIterator.next();
			return true;
		} else {
			nextPreferenceName = null;
			return false;
		}
	}

	private void setPrefSet(Stream<String> pref) {
		preferenceIterator = pref.iterator();
		nextPreferenceElement();
	}

	private void logProgress() {
		LOG.info("Processed {} nodes, found {} preference elements", recordsProcessed, preferenceNamesFound);
	}

	private long readJoinNode(String line) {
		int sep1 = line.indexOf('\t');
		if (sep1 == -1) {
			return -1;
		}
		lastId = Long.parseLong(line.substring(0, sep1));
		sep1++;
		int sep2 = line.indexOf('\t', sep1);
		if (sep2 == -1) {
			sep2 = line.length();
		}
		String name = line.substring(sep1, sep2);
		long res = -1;
		if (nextPreferenceName != null) {
			int c = name.compareTo(nextPreferenceName);
			while (c > 0 && nextPreferenceElement()) {
				c = name.compareTo(nextPreferenceName);
			}
			if (c == 0) {
				preferenceNamesFound++;
				nextPreferenceElement();
				res = lastId;
			}
		}
		recordsProcessed++;
		if ((recordsProcessed % 1000000) == 0) {
			logProgress();
		}
		return res;
	}

	private Double convertNode(String line) {
		if (readJoinNode(line) < 0) {
			return 0.0;
		}
		return defaultPreferenceValue;
	}

	private void read(Stream<String> in) {
		in.map(this::readJoinNode).forEach(id -> {
			if (id >= 0) {
				preferenceIds.add((long) id);
			}
		});
	}

	private void write(DataOutputStream out) throws IOException {
		long id = 0;
		Iterator<Long> prefIdIter = preferenceIds.iterator();
		long nextPrefId = Long.MAX_VALUE;
		if (prefIdIter.hasNext()) {
			nextPrefId = prefIdIter.next();
		}
		defaultPreferenceValue = 1.0 / preferenceIds.size();
		LOG.info("Preference value = {}", defaultPreferenceValue);
		while (id <= lastId) {
			double res = 0.0;
			if (id == nextPrefId) {
				res = defaultPreferenceValue;
				if (prefIdIter.hasNext()) {
					nextPrefId = prefIdIter.next();
				} else {
					nextPrefId = Long.MAX_VALUE;
				}
			}
			out.writeDouble(res);
			id++;
			if ((id % 1000000) == 0) {
				LOG.info("{}% of preference vector written", String.format("%.2f", (100.0 * id / lastId)));
			}
		}
	}

	private void convert(Stream<String> in, DataOutputStream out) {
		in.map(this::convertNode).filter(Objects::nonNull).forEach(t -> {
			try {
				out.writeDouble(t);
			} catch (IOException e) {
				LOG.error("Failed to write preference vector:", e);
				System.exit(1);
			}
		});
	}

	/**
	 * Check preference vector whether values sum up to 1.0, see <a href=
	 * "http://law.di.unimi.it/software/law-docs/it/unimi/dsi/law/rank/SpectralRanking.html#isStochastic-it.unimi.dsi.fastutil.doubles.DoubleList-">isStochastic()</a>
	 */
	private boolean validatePreferenceVector() {
		double sumPreferenceValues = preferenceNamesFound * defaultPreferenceValue;
		if (Math.abs(sumPreferenceValues - 1.0) > 1E-6) {
			LOG.error("Sum of preference values not within tolerance: abs({} - 1.0) > {}", sumPreferenceValues, 1E-6);
			return false;
		}
		return true;
	}

	private static void showHelp() {
		System.err.println(
				"CreatePreferenceVector [--value <preference_value>] <vertices> <preference_set> <preference_vector>");
		System.err.println("");
		System.err.println("Options:");
		System.err.println(" --value <preference_value>\tprecalculated preference value");
		System.err.println("                           \t1/n for n preferred vertices)\");");
		System.err.println("If no preference value is given, the preference set is kept");
		System.err.println("in memory, and the preference value is calculated using");
		System.err.println("the number of found preference elements");
		System.err.println("");
		System.err.println("Input / output parameters");
		System.err.println(" <vertices>\tvertices file with format:");
		System.err.println("           \t  <id> \\t <name>");
		System.err.println(" <preference_set>\tfile containing set of \"preferred\" vertices,");
		System.err.println("                 \tone vertex <name> per line");
		System.err.println(" <preference_vector>\toutput file, binary preference vector,");
		System.err.println("                    \tused as \"--preference-vector\"");
		System.err.println("                    \tfor the LAW PageRank classes");
		System.err.println("Both input files, vertices and preference set, must be sorted");
		System.err.println("lexicographically by vertex names, vertex ids are assigned");
		System.err.println("in sequential order starting from 0.");
		System.err.println("");
	}

	public static void main(String[] args) {
		double defaultPrefVal = 0.0;
		boolean inMemory = true;
		int argpos = 0;
		while (argpos < args.length && args[argpos].startsWith("-")) {
			switch (args[argpos]) {
			case "--value":
				try {
					defaultPrefVal = Double.parseDouble(args[++argpos]);
				} catch (NumberFormatException e) {
					LOG.error("Invalid number: " + args[argpos]);
					System.exit(1);
				}
				inMemory = false;
				break;
			default:
				System.err.println("Unknown option " + args[argpos]);
				showHelp();
				System.exit(1);
			}
			argpos++;
		}

		if (args.length < 3) {
			showHelp();
			System.exit(1);
		}
		String nodesIn = args[argpos++];
		String prefSet = args[argpos++];
		String prefOut = args[argpos++];

		CreatePreferenceVector converter = new CreatePreferenceVector(defaultPrefVal);

		try (Stream<String> in = Files.lines(Paths.get(nodesIn));
				Stream<String> pref = Files.lines(Paths.get(prefSet))) {
			DataOutputStream out;
			if (prefOut.equals("-")) {
				out = new DataOutputStream(System.out);
			} else {
				out = new DataOutputStream(Files.newOutputStream(Paths.get(prefOut)));
			}
			converter.setPrefSet(pref);
			if (inMemory) {
				LOG.info("Reading preference vector...");
				converter.read(in);
				LOG.info("Writing preference vector...");
				converter.write(out);
			} else {
				LOG.info("Converting preference vector...");
				converter.convert(in, out);
			}
			converter.logProgress();
			if (!converter.validatePreferenceVector()) {
				System.exit(2);
			}
		} catch (IOException e) {
			LOG.error("Failed to create preference vector:", e);
			System.exit(1);
		}
	}

}