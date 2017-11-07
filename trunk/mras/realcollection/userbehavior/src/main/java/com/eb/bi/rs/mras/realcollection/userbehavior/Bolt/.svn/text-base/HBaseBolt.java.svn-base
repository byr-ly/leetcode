package com.eb.bi.rs.mras.realcollection.userbehavior.Bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mras.realcollection.userbehavior.util.FName;
import com.eb.bi.rs.mras.realcollection.userbehavior.util.StreamId;
import com.eb.bi.rs.mras.realcollection.userbehavior.util.TimeParaser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by LiMingji on 2015/9/6.
 */
public class HBaseBolt extends BaseBasicBolt {

    private static Logger log = Logger.getLogger(HBaseBolt.class);

    private static String HBASE_ZK;
    private static String HBASE_ZKPORT;
    private static String HBASE_TABLE_NAME;
    private static String HBASE_TABLE_CF;

    private static Configuration conf = null;
    private static transient HTable hTable;

    private ArrayList<Put> putsList = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        log.error("===> prepared");
        super.prepare(stormConf, context);
        PluginConfig appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());
        HBASE_ZK = appConf.getParam("HBASE_ZK");
        HBASE_ZKPORT = appConf.getParam("HBASE_ZKPORT");
        HBASE_TABLE_NAME = appConf.getParam("HBASE_TABLE_NAME");
        HBASE_TABLE_CF = appConf.getParam("HBASE_TABLE_CF");

        conf = HBaseConfiguration.create();
        if (HBASE_ZK == null || HBASE_ZK.equals("")) {
            HBASE_ZK = "ZJHZ-RTS-NN1,ZJHZ-RTS-NN2,ZJHZ-RTS-DN1,ZJHZ-RTS-DN2,ZJHZ-RTS-DN3";
        }
        if (HBASE_ZKPORT == null || HBASE_ZKPORT.equals("")) {
            HBASE_ZKPORT = "2181";
        }
        conf.set("hbase.zookeeper.quorum", HBASE_ZK);
        conf.set("hbase.zookeeper.property.clientPort", HBASE_ZKPORT);

        try {
            hTable = new HTable(conf, HBASE_TABLE_NAME);
        } catch (IOException e) {
            log.error("HBase init error: " + e);
        }
        if (hTable != null) {
            log.error("init htable succeed");
        } else {
            log.error("init htable error");
        }
        putsList = new ArrayList<Put>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Put put = null;
        String rowKey = null;
        if (tuple.getSourceStreamId().equals(StreamId.BROWSEDATA.name())) {
            String msisdn = tuple.getStringByField(FName.MSISDN.name());
            Long recordTime = tuple.getLongByField(FName.RECORDTIME.name());
            String bookId = tuple.getStringByField(FName.BOOKID.name());
            String pageType = tuple.getStringByField(FName.PAGETPYE.name());

            String type ;
            try {
                //2为浏览
                if (Integer.parseInt(pageType) == 2) {
                    type = "pv";
                }
                //3为阅读
                else if (Integer.parseInt(pageType) == 3) {
                    type = "read";
                } else {
                    return;
                }
            } catch (NumberFormatException e) {
                return ;
            }

            rowKey = msisdn + "|" + bookId + "|" + type;
            put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(HBASE_TABLE_CF), Bytes.toBytes("time"), Bytes.toBytes(TimeParaser.formatTimeInSeconds(recordTime)));

        } else if (tuple.getSourceStreamId().equals(StreamId.ORDERDATA.name())) {
            String msisdn = tuple.getStringByField(FName.MSISDN.name());
            Long recordTime = tuple.getLongByField(FName.RECORDTIME.name());
            String bookId = tuple.getStringByField(FName.BOOKID.name());

            rowKey = msisdn + "|" + bookId+"|order";
            put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(HBASE_TABLE_CF), Bytes.toBytes("time"), Bytes.toBytes(TimeParaser.formatTimeInSeconds(recordTime)));
        }

        if (put == null || rowKey == null) {
            log.error("===> rowKey or put == null ");
            return ;
        }
        putsList.add(put);
        try {
            if (putsList.size() >= 1000) {
                hTable.put(putsList);
                log.error("===> insert data " + putsList.size());
                putsList.clear();
            }
        } catch (IOException e) {
            log.error("Insert data error: " + rowKey);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //pass
    }
}
