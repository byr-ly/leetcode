package com.eb.bi.rs.andedu.hdfs2hbase.base;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Job的子部件，组合模式中的Composite
 */
public class JobComposite extends JobComponent {

    private static final Logger LOG = Logger.getLogger(JobComposite.class);
    private List<JobComponent> jobList;

    public JobComposite(String name) {
        super(name);

        jobList = new ArrayList<JobComponent>();
    }

    public void add(JobComponent jobComponent) {
        jobList.add(jobComponent);
    }

    public void remove(JobComponent jobComponent) {
        jobList.remove(jobComponent);
    }

    public int run(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        LOG.info("Job " + getName() + " begin...");

        int ret = 0;
        for (JobComponent job : jobList) {
            if (job.run(args) != 0) {
                ret = -1;
                break;
            }
        }

        double cost = (System.currentTimeMillis() - begin) / 1000;
        String desc = (ret == 0 ? "successed" : "failed");
        LOG.info("Job " + getName() + " " + desc + ". Time cost: " + cost + "s.");

        return ret;
    }
}
