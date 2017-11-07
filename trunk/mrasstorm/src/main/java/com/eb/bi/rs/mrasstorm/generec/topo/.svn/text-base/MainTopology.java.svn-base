package com.eb.bi.rs.mrasstorm.generec.topo;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mrasstorm.generec.bolt.ActionBolt;
import com.eb.bi.rs.mrasstorm.generec.bolt.MergeBoltSimilar;
import com.eb.bi.rs.mrasstorm.generec.bolt.RequestSpout;
import com.eb.bi.rs.mrasstorm.generec.bolt.SimilarBolt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ynn
 * @version 1.0
 * @date 创建时间：2015-10-26 上午10:16:34
 */
public class MainTopology {

    public static void main(String[] args) throws Exception {
        Date dateBeg = new Date();
        PluginUtil pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        PluginConfig pluginConfig = pluginUtil.getConfig();

        int workerNum = pluginConfig.getParam("worker_num", 5);
        int spoutNum = pluginConfig.getParam("spout_num", 1);
        int boltNum = pluginConfig.getParam("bolt_num", 1);
        int ackerNum = pluginConfig.getParam("acker_num", 1);
        int maxSpoutPending = pluginConfig.getParam("max_spout_pending", 10000);
        
        String topoName = pluginConfig.getParam("topo_name", "generec-realtimerec-topology");

        // storm configuration
        Config conf = new Config();
        conf.setMaxSpoutPending(maxSpoutPending);
        conf.setNumWorkers(workerNum);
        conf.setNumAckers(ackerNum);

        try {
            conf.put("AppConfig",
                    appConfigStr(pluginConfig.getConfigFilePath()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("request-spout", new RequestSpout(), spoutNum);
        builder.setBolt("similar-bolt", new SimilarBolt(), boltNum)
                .shuffleGrouping("request-spout");
        builder.setBolt("merge-bolt", new MergeBoltSimilar(),boltNum)
                .shuffleGrouping("similar-bolt");
        builder.setBolt("action-bolt", new ActionBolt(), boltNum)
                .shuffleGrouping("request-spout");

        String exitDesc = "run successfully";
        int exitCode = PluginExitCode.PE_SUCC;

        try {
            StormSubmitter.submitTopology(topoName, conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
            exitDesc = "submit topology already alive";
            exitCode = PluginExitCode.PE_EXEC_ERR;
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
            exitDesc = "submit topology is invalid";
            exitCode = PluginExitCode.PE_EXEC_ERR;
        }

        // idox写入运行结果
        Date dateEnd = new Date();
        String endTime = new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd);
        long timeCost = dateEnd.getTime() - dateBeg.getTime();

        PluginResult result = pluginUtil.getResult();
        result.setParam("endTime", endTime);
        result.setParam("timeCosts", timeCost);
        result.setParam("exitCode", exitCode);
        result.setParam("exitDesc", exitDesc);
        result.save();
    }

    public static String appConfigStr(String confFilePath) throws IOException {
        String data = "";
        String line = "";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(confFilePath));
            while ((line = br.readLine()) != null) {
                data += line;
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
        return data;
    }
}

