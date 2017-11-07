package com.eb.bi.rs.andedu.hdfs2hbase.base;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.util.Properties;

public abstract class BaseDriver extends Configured implements Tool {

    protected Properties properties;

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}

