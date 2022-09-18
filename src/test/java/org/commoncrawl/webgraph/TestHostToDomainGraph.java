/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2022 Common Crawl and contributors
 */
package org.commoncrawl.webgraph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.commoncrawl.webgraph.HostToDomainGraph.Domain;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestHostToDomainGraph {

	protected static Logger LOG = LoggerFactory.getLogger(TestHostToDomainGraph.class);

	static final int maxGraphNodes = 128;

	HostToDomainGraph converter;

	String[] hostGraphSimple = { //
			"0\tcom.example", //
			"1\tcom.example.www,", //
			"2\tcom.example.xyz,", //
			"3\torg.example" //
	};
	String[] domainGraphSimple = { //
			"0\tcom.example\t3", //
			"1\torg.example\t1" //
	};

	String[] hostGraphNamesNotSorted = { //
			"0\tcom.example", //
			"1\tcom.example.xyz,", //
			"2\tcom.example.www,", //
			"3\torg.example" //
	};

	String[] hostGraphHyphenatedDomains = { //
			"0\tac.e-bike", //
			"1\tac.e-bikes", //
			"2\tac.e-com", //
			"3\tac.e.subdomain", //
			"4\tac.eagle", //
			"5\tac.gov", // domain name public suffix only
			"6\tac.gov.ascension", //
			"7\tac.gov.ascension-island", //
			"8\tac.gov.ascension.mail", //
			"9\tac.gov.conservation-ascension-island", //
			"10\tac.gov.postoffice", //
	};
	String[] domainGraphHyphenatedDomains = { //
			"0\tac.e\t1", //
			"1\tac.e-bike\t1", //
			"2\tac.e-bikes\t1", //
			"3\tac.e-com\t1", //
			"4\tac.eagle\t1", //
			"5\tac.gov.ascension\t2", //
			"6\tac.gov.ascension-island\t1", //
			"7\tac.gov.conservation-ascension-island\t1", //
			"8\tac.gov.postoffice\t1", //
	};
	String[] domainGraphHyphenatedDomainsInclMultiPartSuffixes = { //
			"0\tac.e\t1", //
			"1\tac.e-bike\t1", //
			"2\tac.e-bikes\t1", //
			"3\tac.e-com\t1", //
			"4\tac.eagle\t1", //
			"5\tac.gov\t1", //
			"6\tac.gov.ascension\t2", //
			"7\tac.gov.ascension-island\t1", //
			"8\tac.gov.conservation-ascension-island\t1", //
			"9\tac.gov.postoffice\t1", //
	};

	String[] hostGraphHyphenatedDomainsSubDomainOnly = { //
			"0\tac.gov.ascension-island", //
			"1\tac.gov.ascension.mail", //
			"2\tac.gov.conservation-ascension-island", //
			"3\tac.gov.postoffice", //
	};
	String[] domainGraphHyphenatedDomainsSubDomainOnly = { //
			"0\tac.gov.ascension\t1", //
			"1\tac.gov.ascension-island\t1", //
			"2\tac.gov.conservation-ascension-island\t1", //
			"3\tac.gov.postoffice\t1", //
	};

	String[] hostGraphDuplicatedDomains = { //
			"0\tno.hordaland", //
			"1\tno.hordaland-teater", //
			"2\tno.hordaland.os", //
			"3\tno.hordaland.os.bibliotek", //
			"4\tno.hordaland.oygarden", //
			"5\tno.hordalandfolkemusikklag", //
	};
	String[] domainGraphDuplicatedDomains = { //
			"0\tno.hordaland\t2", //
			"1\tno.hordaland-teater\t1", //
			"2\tno.hordaland.os.bibliotek\t1", //
			"3\tno.hordalandfolkemusikklag\t1", //
	};

	@BeforeEach
	void init() {
		converter = new HostToDomainGraph(maxGraphNodes);
	}

	@Test
	void testDomainComparison() {
		assertTrue("org.example.".compareTo("org.example-domain.") > 0);
		assertTrue(Domain.compareRevDomainsSafe("org.example", "org.example") == 0);
		assertTrue(Domain.compareRevDomainsSafe("org.example", "org.exampledomain") < 0);
		assertTrue(Domain.compareRevDomainsSafe("org.example", "org.example-domain") > 0);
		assertTrue(Domain.compareRevDomainsSafe("org.example", "org.example.domain") > 0);
	}

	private String[] convert(HostToDomainGraph converter, String[] hostGraph) {
		ByteArrayOutputStream domainBytes = new ByteArrayOutputStream();
		PrintStream domainOut = new PrintStream(domainBytes);
		converter.convert(converter::convertNode, Arrays.stream(hostGraph), domainOut);
		converter.finishNodes(domainOut);
		return new String(domainBytes.toByteArray(), StandardCharsets.UTF_8).split("\n");
	}

	private String[] stripCounts(String[] domainGraph) {
		return Arrays.stream(domainGraph).map(s -> s.replaceFirst("\\t[^\\t]*$", "")).toArray(String[]::new);
	}

	private String[] getNodeNames(String[] graph) {
		return Arrays.stream(graph).map(s -> s.split("\t")[1]).toArray(String[]::new);
	}

	private long[] getNodeIDs(String[] graph) {
		return Arrays.stream(graph).mapToLong(s -> Long.parseLong(s.split("\t")[0])).toArray();
	}

	/**
	 * test whether node names are properly sorted and IDs are correctly assigned
	 * (sequentially, strictly monotonically increasing, no gaps)
	 */
	void testSorted(String[] graph) {
		String[] names = getNodeNames(graph);
		String[] namesSorted = Arrays.copyOf(names, names.length);
		Arrays.sort(namesSorted);
		assertArrayEquals(namesSorted, names);
		long lastId = -1;
		for (long id : getNodeIDs(graph)) {
			if ((lastId + 1) != id) {
				fail("IDs not correctly assigned: " + lastId + ", " + id);
			}
			lastId = id;
		}
	}

	@Test
	void testConvertNodesSimple() {
		testSorted(hostGraphSimple);
		converter.doCount(false);
		assertArrayEquals(stripCounts(domainGraphSimple), convert(converter, hostGraphSimple));
		testSorted(domainGraphSimple);
	}

	@Test
	void testConvertNodesSimpleCount() {
		converter.doCount(true);
		assertArrayEquals(domainGraphSimple, convert(converter, hostGraphSimple));
	}

	@Test
	void testConvertNodesNotSorted() {
		try {
			convert(converter, hostGraphNamesNotSorted);
			fail("Unable to convert to domain graph from not properly sorted input");
		} catch (Exception e) {
			LOG.info("Expected exception on input not properly sorted", e.getMessage());
		}
	}

	@Test
	void testConvertNodesHyphenatedDomains() {
		// verify sorting of input and expected output
		testSorted(hostGraphHyphenatedDomains);
		testSorted(domainGraphHyphenatedDomains);
		converter.doCount(true);
		assertArrayEquals(domainGraphHyphenatedDomains, convert(converter, hostGraphHyphenatedDomains));
	}

	@Test
	void testConvertNodesHyphenatedDomainsSubDomainOnly() {
		// verify sorting of input and expected output
		testSorted(hostGraphHyphenatedDomainsSubDomainOnly);
		testSorted(domainGraphHyphenatedDomains);
		converter.doCount(true);
		assertArrayEquals(domainGraphHyphenatedDomainsSubDomainOnly,
				convert(converter, hostGraphHyphenatedDomainsSubDomainOnly));
	}

	@Test
	void testConvertNodesDuplicatedDomain() {
		// verify sorting of input and expected output
		testSorted(hostGraphDuplicatedDomains);
		testSorted(domainGraphDuplicatedDomains);
		converter.doCount(true);
		assertArrayEquals(domainGraphDuplicatedDomains, convert(converter, hostGraphDuplicatedDomains));
	}

	@Test
	void testConvertNodesHyphenatedDomainsIncludingMultiPartSuffixes() {
		// verify sorting of input and expected output
		testSorted(hostGraphHyphenatedDomains);
		testSorted(domainGraphHyphenatedDomainsInclMultiPartSuffixes);
		converter.doCount(true);
		converter.multiPartSuffixesAsDomains(true);
		assertArrayEquals(domainGraphHyphenatedDomainsInclMultiPartSuffixes,
				convert(converter, hostGraphHyphenatedDomains));
	}

}
