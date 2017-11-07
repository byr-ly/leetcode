package com.eb.bi.rs.frame.recframe.base;

import org.apache.log4j.Logger;

/**
 * 组合模式中的Leaf
 */
class TestLeaf extends JobComponent {

	private static final Logger LOG = Logger.getLogger(TestLeaf.class);

	public TestLeaf(String name) {
		super(name);
	}

	public int run(String[] args) throws Exception {
		LOG.info("Job " + getName() + " running...");
		return 0;
	}
}
