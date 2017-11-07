package com.eb.bi.rs.mrasstorm.realcollection.userbehavior.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.util.FName;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.util.StreamId;
import com.eb.bi.rs.mrasstorm.realcollection.userbehavior.util.TimeParaser;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;

/**
 * 订购话单Topic
 *
 * 话单格式：（接受消息）
 * {"body":{"cdr":"42000012018|20150505165523||1|4|10928||140021346||0000|140021344|
 * 140021346||140021346|150|150|0|0|42000012018|3|||250|25|127.0.0.1|4|1||||||13776640821
 * ||||1|||||221.226.57.202|||||13776640821||13776640821|"},"seqid":"1","tags":["ireadcharge11"],
 * "topic":"report.cdr","type":"report.cdr"}
 *
 * 需要获取的字段：（发射消息）
 *  0. msisdn      |   发起人身份ID
 *  1. recordTime  |   记录时间
 *  2. terminal    |   终端名称
 *  3. platform    |   门户类型
 *  4. OrderType   |   订购类型 1-按本  2-按章 4-包月 5-促销包
 *  5. ProductID   |   产品ID
 *  7. BookID      |   图书ID
 *  8. ChapterID   |   章节ID
 *  9. ChannelCode |   渠道ID
 * 14. RealInfoFee |   真实信息费
 * 22. provinceID  |   手机号对应的省ID    （20150527新增）
 * 24. WapIp       |   终端或网关IP
 * 39. SessionId   |   会话ID
 * 40. PromotionId |   促销活动ID
 *
 * Created by LiMingji on 2015/09/06.
 */
public class OrderSplitBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    static Logger log = Logger.getLogger(OrderSplitBolt.class);

    ////将订单数据从json中解析出来
    private String splitJson(String msg) {
        try {
            JSONObject msgJson = new JSONObject(msg);
            Iterator<String> msgIt = msgJson.keys();
            while (msgIt.hasNext()) {
                String msgKey = msgIt.next();
                if (msgKey.equals("body")) {
                    JSONObject cdrJson = new JSONObject(msgJson.getString(msgKey));
                    Iterator<String> cdrIt = cdrJson.keys();
                    while (cdrIt.hasNext()) {
                        String cdrkey = cdrIt.next();
                        if (cdrkey.equals("cdr")) {
                            return cdrJson.getString(cdrkey);
                        }
                    }
                }
            }
        } catch (JSONException e) {
            log.error("订单消息格式错误" + msg);
        }
        return null;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = splitJson(input.getString(0));
        if (line == null) {
            return ;
        }
        String[] words = line.split("\\|", -1);
        if (words.length >= 49) {
            String msisdn = words[0]; // msisdnID Varchar2(20)
            String recordTime = words[1]; // Recordtime Varchar2(14)
            String bookID = words[7]; // 图书ID Number(19)

            Long orderTime = TimeParaser.splitTime(recordTime);

            collector.emit(StreamId.ORDERDATA.name(), new Values(msisdn, orderTime, bookID));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StreamId.ORDERDATA.name(),
                new Fields(FName.MSISDN.name(), FName.RECORDTIME.name(), FName.BOOKID.name()));
    }
}
