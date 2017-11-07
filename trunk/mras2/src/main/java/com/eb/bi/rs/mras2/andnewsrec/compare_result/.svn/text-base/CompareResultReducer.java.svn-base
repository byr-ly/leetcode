package com.eb.bi.rs.mras2.andnewsrec.compare_result;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by LiMingji on 2016/4/18.
 */
public class CompareResultReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    //Logger log = PluginUtil.getInstance().getLogger();
    Producer<String, String> producer = null;
    String returnTopic = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration hadoopConf = context.getConfiguration();
        String zkConn = hadoopConf.get("zkCfg", "ZJHZ-RTS-DN11:2181,ZJHZ-RTS-DN12:2181,ZJHZ-RTS-DN13:2181,ZJHZ-RTS-DN14:2181");
        String groupId = hadoopConf.get("group_id", "test-consumer-group");
        String brokerList = hadoopConf.get("broker_list", "ZJHZ-RTS-DN11:9092,ZJHZ-RTS-DN12:9092,ZJHZ-RTS-DN13:9092,ZJHZ-RTS-DN14:9092");
        returnTopic = hadoopConf.get("returnTopic", "AndNews.SimilarNews");

        Properties props = new Properties();
        props.put("zookeeper.connect", zkConn);
        props.put("group.id", groupId);
        props.put("metadata.broker.list", brokerList);
        props.put("zookeeper.session.timeout.ms", "600");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    private void emitMsg2Kafka(String key, String message) {
        String newsID = null;
        try {
            newsID = key.toString().split("\\|")[0];
        } catch (Exception e) {
        }
        JSONObject obj = new JSONObject();
        try {
            obj.accumulate("contid", newsID);
            obj.accumulate("similar_news", message);
            obj.accumulate("reserve", "");
            System.out.println("emit id " + newsID);
            System.out.println("emit msg: " + message);
            //log.info("#####################################" + obj.toString());
            producer.send(new KeyedMessage<String, String>(returnTopic, obj.toString()));
        } catch (JSONException e) {

        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String yesterdayNewRet = null;
        ArrayList<String> todayNewsRet = new ArrayList<String>();
        for (Text value : values) {
            String[] valueFields = value.toString().split("@", -1);
            if (valueFields.length < 2) {
                continue;
            }
            String tag = valueFields[0];
            if (tag.equals("today")) {
                todayNewsRet.add(valueFields[1]);
            } else if (tag.equals("yesterday")) {
                yesterdayNewRet = valueFields[1];
            }
        }
        System.out.println(yesterdayNewRet + " ===== " + todayNewsRet);
        if (yesterdayNewRet == null || yesterdayNewRet.isEmpty()) {
            for (String messsage : todayNewsRet) {
                emitMsg2Kafka(key.toString(), messsage);
            }
        } else {
            for (String message : todayNewsRet) {
                if (message.equals(yesterdayNewRet)) {
                    continue;
                }
                emitMsg2Kafka(key.toString(), message);
            }
        }
    }
}

