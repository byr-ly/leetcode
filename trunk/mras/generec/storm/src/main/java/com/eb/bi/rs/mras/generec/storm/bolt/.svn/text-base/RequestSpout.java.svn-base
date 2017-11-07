package com.eb.bi.rs.mras.generec.storm.bolt;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.datainput.DataRecord;
import com.eb.bi.rs.frame.common.storm.datainput.InputMsgManager;
import com.eb.bi.rs.frame.common.storm.datainput.LoaderBase;

import java.util.Map;

public class RequestSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private LoaderBase dataLoader;
    private int msgId;

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        PluginConfig appConf = ConfigReader.getInstance().initConfig(
                conf.get("AppConfig").toString());
        InputMsgManager inputMsgManager = InputMsgManager.getInstance();
        inputMsgManager.init(appConf);
        dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
        while (dataLoader == null) {
            dataLoader = inputMsgManager
                    .getLoader(context.getThisComponentId());
        }
    }

    public void nextTuple() {
        DataRecord record = dataLoader.getRecord();
        if (record == null) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        // userid=用户ID&gene_id=基因1ID,基因2ID (一个或多个)
        String user = record.getField("userid").getField();
        String gene = record.getField("gene_id").getField();
        PrintHelper.print("userid:" + user + ">> gene_id:" + gene);

        if (!user.isEmpty() && !gene.isEmpty()) {
            collector.emit(new Values(user, gene), msgId++);
        } else {
            PrintHelper.print("userid or gene is empty");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "gene"));
    }
}
