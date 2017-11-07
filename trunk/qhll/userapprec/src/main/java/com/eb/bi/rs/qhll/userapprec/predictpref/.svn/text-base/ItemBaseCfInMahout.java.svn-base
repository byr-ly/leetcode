package com.eb.bi.rs.qhll.userapprec.predictpref;

import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

public class ItemBaseCfInMahout {
    public void runMahout(String[] args) throws Exception {
  	
    	String inPath = args[3];
    	String outPath = args[4];
    	String tmpPath =  args[5];
        JobConf conf = config();

        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        sb.append(" --booleanData" + args[7]);
        // 近邻个数
        sb.append(" --maxSimilaritiesPerItem " + args[8]);
        // 推荐物品数
        sb.append(" --numRecommendations " + args[9]);
        // CooccurrenceCountSimilarity CosineSimilarity  EuclideanDistanceSimilarity PearsonCorrelationSimilarity
        sb.append(" --similarityClassname " + args[10]);
        sb.append(" --tempDir ").append(tmpPath);
        String[] sbArgs = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        job.setConf(conf);
        job.run(sbArgs);
    }

    public static JobConf config() 
    {
        JobConf conf = new JobConf(ItemBaseCfInMahout.class);
        conf.setJobName("ItemCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}
