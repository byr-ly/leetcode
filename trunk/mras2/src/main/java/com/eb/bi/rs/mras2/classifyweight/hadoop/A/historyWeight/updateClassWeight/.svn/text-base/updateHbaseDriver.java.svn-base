package com.eb.bi.rs.mras2.classifyweight.hadoop.A.historyWeight.updateClassWeight;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

/**更新分类权重表
 * Created by linwanying on 2017/6/12.
 */
public class updateHbaseDriver extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public updateHbaseDriver(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new updateHbaseDriver(args), args);

        Date end = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String endTime = format.format(end);
        long timeCost = end.getTime() - begin.getTime();

        PluginResult result = pluginUtil.getResult();
        result.setParam("endTime", endTime);
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
        result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
        result.save();

        log.info("time cost in total(s): " + (timeCost / 1000.0));
        System.exit(ret);
    }

    @Override
    public int run(String[] args) throws Exception {
        PluginConfig config = pluginUtil.getConfig();
        Configuration conf;
        Job job;
        long start;

        /**
         * ********************************************************************************************
         * MAP REDUCE JOB:
         ** 输入：
         **		输入路径：用户分类权重hbase表。
         ** 输出：
         **		用户分类权重hbase表。
         **
         **
         *******************************************************************************************/
        start = System.currentTimeMillis();
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", config.getParam("hbase.zookeeper.quorum", "ZJHZ-RTS-NN1,ZJHZ-RTS-NN2,ZJHZ-RTS-DN1,ZJHZ-RTS-DN2,ZJHZ-RTS-DN3"));
        conf.set("hbase.zookeeper.property.clientPort", config.getParam("hbase.zookeeper.property.clientPort", "2181"));
        conf.set("class_weight_table", config.getParam("class_weight_table", "unify_user_classid_weight"));//输出表1
        conf.set("excitation_weight_table", config.getParam("excitation_weight_table", "unify_user_excitation_weight"));//输出表2
        job = Job.getInstance(conf, "updateHbase");
        job.setJarByClass(updateHbaseDriver.class);


        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
//        //指定要查询的列
//        scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column"));
//        TableMapReduceUtil.initMultiTableSnapshotMapperJob(scans, readWeightMapper.class, Text.class, Text.class, job, false, new Path(""));
        TableMapReduceUtil.initTableMapperJob(config.getParam("excitation_weight_table", "unify_user_excitation_weight"), scan, readWeightMapper.class, Text.class, Text.class, job);
        MultipleInputs.addInputPath(job, new Path(config.getParam("history_weight_path", "/user/hadoop/linwanying/classifyweight/result/user_class_normalize")), TextInputFormat.class, readHistoryMapper.class);
        MultipleInputs.addInputPath(job, new Path(config.getParam("class_weight_path", "/user/recsys/generec/output/class_weight")), TableInputFormat.class, readWeightMapper.class);
        //设置输出类型(map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(putWeightReducer.class);
        //设置输出类型(reduce)
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
//        TableMapReduceUtil.initTableReducerJob(config.getParam("user_class_weight", "unify_user_excitation_weight"), putWeightReducer.class, job);
        job.setNumReduceTasks(Integer.parseInt(config.getParam("tablemap_reduce_num", "50")));
        if (job.waitForCompletion(true)) {
            log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
        } else {
            log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
            return 1;
        }
        return 0;
    }
}
