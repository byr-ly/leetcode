package com.eb.bi.rs.frame.recframe.base;

import org.apache.hadoop.util.Tool;

import org.apache.log4j.Logger;

class TestTool extends BaseDriver {

	private static final Logger LOG = Logger.getLogger(TestTool.class);

	@Override
	public int run(String[] args) throws Exception {
		LOG.info(String.format("run TestTool with " + properties));
		return 0;
	}
}

