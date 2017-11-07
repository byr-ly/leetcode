package com.eb.bi.rs.mrasstorm.realcollection.userbehavior.topo;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.bolt.HBaseBolt;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.bolt.OrderSplitBolt;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.bolt.PageviewSplitBolt;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.util.StreamId;
import storm.kafka.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LiMingji on 2015/9/9.
 */
public class MainTopo {

    static Logger log = Logger.getLogger(MainTopo.class);

    public static void main(String[] args) {
        /*
        * 读取storm相关配置
        * */
        Date dateBeg = new Date();
        PluginUtil pluginUtil = PluginUtil.getInstance();
        pluginUtil.init(args);
        PluginConfig pluginConfig = pluginUtil.getConfig();

        int workerNum = pluginConfig.getParam("worker_num", 10);
        int maxSpoutPending = pluginConfig.getParam("max_spout_pending", 10000);

        //storm Bolt个数参数
        int page_spout_num = pluginConfig.getParam("page_spout_num", 16);
        int page_split_bolt_num = pluginConfig.getParam("page_split_bolt_num", 50);
        int order_spout_num = pluginConfig.getParam("order_spout_num", 8);
        int order_split_bolt_num = pluginConfig.getParam("order_split_bolt_num", 5);
        int hbase_bolt_num = pluginConfig.getParam("hbase_bolt_num", 10);

        String zkCfg = pluginConfig.getParam("zkCfg",
                "ZJHZ-CMREAD-ZOOKEEPER1-VBUS-SD:6830," +
                        "ZJHZ-CMREAD-ZOOKEEPER2-VBUS-SD:6830," +
                        "ZJHZ-CMREAD-ZOOKEEPER3-VBUS-SD:6830");

        String pageviewTopic = pluginConfig.getParam("pageviewTopic", "Portal.Pageview");
        String orderTopic = pluginConfig.getParam("orderTopic", "report.cdr");
        String zkRoot = pluginConfig.getParam("zkRoot", "/ebupt-userbehavior");
        /*
        * 设置storm相关配置。
        * */
        Config conf = new Config();
        conf.setMaxSpoutPending(maxSpoutPending);
        conf.setNumWorkers(workerNum);
        conf.setNumAckers(0);
        conf.setMessageTimeoutSecs(60000);
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

        try {
            conf.put("AppConfig", appConfigStr(pluginConfig.getConfigFilePath()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (pageviewTopic.equals("") || orderTopic.equals("")) {
            log.error("Kafka's topic is less than 2.");
            System.exit(1);
        }

        /**
         *  构建storm Topology
         */
        BrokerHosts brokerHosts = new ZkHosts(zkCfg);
        //浏览话单
        SpoutConfig pageViewSpoutConfigTopic = new SpoutConfig(brokerHosts, pageviewTopic, zkRoot, "pageview");
        pageViewSpoutConfigTopic.scheme = new SchemeAsMultiScheme(new StringScheme());
        pageViewSpoutConfigTopic.forceFromStart = false;
        pageViewSpoutConfigTopic.socketTimeoutMs = 60000;

        //订购话单
        SpoutConfig orderSpoutConfigTopic = new SpoutConfig(brokerHosts, orderTopic, zkRoot, "order");
        orderSpoutConfigTopic.scheme = new SchemeAsMultiScheme(new StringScheme());
        orderSpoutConfigTopic.forceFromStart = false;
        orderSpoutConfigTopic.socketTimeoutMs = 60000;

        TopologyBuilder builder = new TopologyBuilder();
        //浏览话单发射、分词bolt
        builder.setSpout(StreamId.Portal_Pageview.name(), new KafkaSpout(pageViewSpoutConfigTopic), page_spout_num);
        builder.setBolt(StreamId.PageViewSplit.name(), new PageviewSplitBolt(), page_split_bolt_num)
                .shuffleGrouping(StreamId.Portal_Pageview.name());

        //订购话单发射、分词bolt
        builder.setSpout(StreamId.report_cdr.name(), new KafkaSpout(orderSpoutConfigTopic), order_spout_num);
        builder.setBolt(StreamId.OrderSplit.name(), new OrderSplitBolt(), order_split_bolt_num)
                .shuffleGrouping(StreamId.report_cdr.name());

        //写入HBaseBolt
        builder.setBolt(StreamId.HBaseBolt.name(), new HBaseBolt(), hbase_bolt_num)
                .shuffleGrouping(StreamId.PageViewSplit.name(), StreamId.BROWSEDATA.name())
                .shuffleGrouping(StreamId.OrderSplit.name(), StreamId.ORDERDATA.name());

        String exitDesc = "run successfully";
        int exitCode = PluginExitCode.PE_SUCC;
        try {
            StormSubmitter.submitTopology("user-behavior-new", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
            exitDesc = "submit topology already alive";
            exitCode = PluginExitCode.PE_EXEC_ERR;
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
            exitDesc = "submit topology is invalid";
            exitCode = PluginExitCode.PE_EXEC_ERR;
        }

        //idox写入运行结果
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
