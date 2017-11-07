package com.eb.bi.rs.opus.hdfs2hbase.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ToolLeaf extends JobComponent {

    private static final Logger LOG = Logger.getLogger(ToolLeaf.class);
    private Tool tool;
    private Configuration conf;

    public ToolLeaf(String name, Tool tool) {
        this(name, tool, tool.getConf());
    }

    public ToolLeaf(String name, Tool tool, Configuration conf) {
        super(name);

        this.tool = tool;
        this.conf = conf;
    }

    public int run(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        LOG.info("Job " + getName() + " begin...");

        int ret = ToolRunner.run(conf, tool, args);

        double cost = (System.currentTimeMillis() - start) / 1000;
        String desc = (ret == 0 ? "successed" : "failed");
        LOG.info("Job " + getName() + " " + desc + ". Time cost: " + cost + "s.");

        return ret;
    }
}
