package com.eb.bi.rs.mras2.new_correlation.offlineTrainingDataPrep;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**read .parquet files
 * Created by lenovo on 2017/7/10.
 */
public class readParquet extends Configured implements Tool {
    private static PluginUtil pluginUtil;
    private static Logger log;

    public readParquet(String[] args) {
        pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        log = pluginUtil.getLogger();
    }

    public static void main(String[] args) throws Exception {
        Date begin = new Date();

        int ret = ToolRunner.run(new Configuration(), new readParquet(args), args);

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
        conf = new Configuration(getConf());

        String inputPath = config.getParam("correlation_gbdttree_input", "");
        String outputPath = config.getParam("correlation_gbdttree_output", "");
        check(outputPath);
        FileStatus[] fsinput = FileSystem.get(conf).globStatus(new Path(inputPath + "/*"));
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fileOutputStream = fs.create(new Path(outputPath));
        ParquetReader<Group> reader = null;
        for (FileStatus status : fsinput) {
            log.info(status.getPath().toString());
            System.out.println(status.getPath().toString());
            reader = ParquetReader.builder(new GroupReadSupport(), new Path(status.getPath().toString())).build();
            Group record;
            while ((record = reader.read()) != null) {
                StringBuilder key = new StringBuilder();
                key.append(record.getInteger("treeId", 0)).append("|")
                        .append(record.getInteger("nodeId", 0)).append("|")
                        .append(record.getGroup("predict", 0).getDouble("predict", 0)).append("|")
                        .append(record.getBoolean("isLeaf", 0)).append("|");
                if (!record.getBoolean("isLeaf", 0)) {
                    key.append(record.getGroup("split", 0).getInteger("feature", 0)).append("|")
                            .append(record.getGroup("split", 0).getDouble("threshold", 0)).append("|")
                            .append(record.getInteger("leftNodeId", 0)).append("|")
                            .append(record.getInteger("rightNodeId", 0));
                }
//                System.out.println(key);
                fileOutputStream.writeChars(key.toString());
                fileOutputStream.writeChars(";");
            }
        }
        reader.close();

        return 0;
    }

    private void check(String fileName) {
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
            Path f = new Path(fileName);
            boolean isExists = fs.exists(f);
            if (isExists) {    //if exists, delete
                boolean isDel = fs.delete(f, true);
                log.info(fileName + "  delete?\t" + isDel);
            } else {
                log.info(fileName + "  exist?\t" + isExists);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
