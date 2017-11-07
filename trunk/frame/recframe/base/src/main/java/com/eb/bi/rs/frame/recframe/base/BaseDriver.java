package com.eb.bi.rs.frame.recframe.base;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.conf.Configured;

import java.util.Properties;

public abstract class BaseDriver extends Configured implements Tool {

	protected Properties properties;

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}

