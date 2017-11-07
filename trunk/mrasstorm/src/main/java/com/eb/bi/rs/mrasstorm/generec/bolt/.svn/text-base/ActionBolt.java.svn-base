package com.eb.bi.rs.mrasstorm.generec.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mrasstorm.generec.util.FName;
import com.eb.bi.rs.mrasstorm.generec.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

/**判断基因操作类型
 * Created by linwanying on 2017/3/23.
 */
public class ActionBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private static Logger log = Logger.getLogger("GeneActionBolt");
    //输入表
    private HTable userClassHistory; // 用户历史分类权重表
    private HTable tagInfo; // 基因分类表
    private HTable pageClass; // 版面分类表
    private HTable pageHotClass; // 版面热门分类表
    private HTable userPageTable; // 用户版面表
    //更新表
    private HTable geneActionTable; // 用户基因表
    private HTable userExcitationWeight; // 用户激发权重表
    private HTable userClassidWeight; // 用户分类权重表

    // 定时器
    private transient Thread loader = null;
    private static int hour;
    private static int min;
    private static long timelag;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        PrintHelper.print("enter the ActionBolt...");

        PluginConfig appConfig = ConfigReader.getInstance().initConfig(
                stormConf.get("AppConfig").toString());
        // 初始化Hbase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                appConfig.getParam("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",
                appConfig.getParam("hbase.zookeeper.property.clientPort"));
        hour = Integer.parseInt(appConfig.getParam("hour"));
        min = Integer.parseInt(appConfig.getParam("minute"));
        timelag = Long.parseLong(appConfig.getParam("timelag"));
        HConnection con = null;
        while (con == null) {
            try {
                con = HConnectionManager.createConnection(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户历史分类表
        while (userClassHistory == null) {
            try {
                userClassHistory = new HTable(conf, appConfig
                        .getParam("user_class_weight_history"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 基因分类表
        while (tagInfo == null) {
            try {
                tagInfo = new HTable(conf, appConfig
                        .getParam("dim_dmn_taginfo"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 版面分类表
        while (pageClass == null) {
            try {
                pageClass = new HTable(conf, appConfig
                        .getParam("dim_class"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 版面热门分类表
        while (pageHotClass == null) {
            try {
                pageHotClass = new HTable(conf, appConfig
                        .getParam("dim_hotclass"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户激发权重表
        while (userExcitationWeight == null) {
            try {
                userExcitationWeight = new HTable(conf, appConfig
                        .getParam("user_excitation_weight"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户分类权重表
        while (userClassidWeight == null) {
            try {
                userClassidWeight = new HTable(conf, appConfig
                        .getParam("user_classid_weight"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户基因表
        while (geneActionTable == null) {
            try {
                geneActionTable = new HTable(conf, appConfig
                        .getParam("user_gene"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户版面表
        while (userPageTable == null) {
            try {
                userPageTable = new HTable(conf, appConfig
                        .getParam("user_page"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 定时器，每天4点更新激发权重表，用户分类权重表
//        if (loader == null) {
//            loader = new Thread(new Runnable() {
//                public void run() {
//                    while (true) {
//                        Scan scan1 = new Scan();
//                        scan1.setBatch(1000);
//                        int count = 0;
////                        scan1.setCaching(100);
//                        try {
//                            ResultScanner rs = userExcitationWeight.getScanner(scan1);
//                            String rowkey, weight, action, latestTime;
//                            double hisweight, exweight, newweight = 0;
//                            long todayMillis, latestMillis;
//                            for (Result result : rs) {
//                                rowkey = Bytes.toString(result.getRow());
//                                action = Bytes.toString(result.getValue(
//                                        Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.action.name())));
//                                weight = Bytes.toString(result.getValue(
//                                        Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name())));
//                                latestTime = Bytes.toString(result.getValue(
//                                        Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.latest_time.name())));
////                                log.info("rowkey: " + rowkey + ",action: " + action + ",weight:" + weight + ",latestTime: " + latestTime);
//                                todayMillis = TimeUtil.getTimeMillis(TimeUtil.getTodayDate());
//                                latestMillis = TimeUtil.getTimeMillis(latestTime);
//                                exweight = Double.parseDouble(weight);
//                                hisweight = Double.valueOf(getHbase(rowkey, FName.weight.name(), userClassHistory));
//                                Put exput = new Put(Bytes.toBytes(rowkey));
//                                Put classput = new Put(Bytes.toBytes(rowkey));
//                                Delete delete = new Delete(Bytes.toBytes(rowkey));
//                                if (action.equals("1")) {
//                                    if (latestMillis < todayMillis) {
//                                        if (todayMillis - latestMillis <= timelag) {
//                                            exweight -= 0.01;
//                                            newweight = Math.max(exweight, hisweight);
//                                            exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()),
//                                                    Bytes.toBytes(String.valueOf(exweight)));
//                                            userExcitationWeight.put(exput);
////                                            log.info("exweight: " + exweight);
//                                        } else {
//                                            exweight -= 0.02;
//                                            if (exweight <= 0) {
//                                                newweight = hisweight;
//                                                userExcitationWeight.delete(delete);
//                                            } else {
//                                                newweight = Math.max(exweight, hisweight);
//                                                exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()),
//                                                        Bytes.toBytes(String.valueOf(exweight)));
//                                                userExcitationWeight.put(exput);
//                                            }
//                                        }
//                                    }
//
//                                } else {
//                                    if (latestMillis < todayMillis) {
//                                        if (todayMillis - latestMillis <= timelag) {
//                                            exweight += 0.01;
//                                            newweight = Math.min(exweight, hisweight);
//                                            exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()),
//                                                    Bytes.toBytes(String.valueOf(exweight)));
//                                            userExcitationWeight.put(exput);
//                                        } else {
//                                            exweight += 0.02;
//                                            if (exweight >= 0.5) {
//                                                newweight = hisweight;
//                                                userExcitationWeight.delete(delete);
//                                            } else {
//                                                newweight = Math.min(exweight, hisweight);
//                                                exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()),
//                                                        Bytes.toBytes(String.valueOf(exweight)));
//                                                userExcitationWeight.put(exput);
//                                            }
//                                        }
//                                    }
//                                }
//                                classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()),
//                                        Bytes.toBytes(String.valueOf(newweight)));
//                                userClassidWeight.put(classput);
//                                count++;
////                                log.info("newweight: " + newweight);
//                            }
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                        log.info("userExcitationWeightTable.size():" + count);
//                        long sleepTime = TimeUtil
//                                .getMillisFromNowToTwelveOclock(hour, min);
//                        if (sleepTime > 0) {
//                            try {
//                                Thread.sleep(sleepTime);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//                }
//            });
//            loader.setDaemon(true);
//            loader.start();
//        }
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        long begin = System.currentTimeMillis();
        String userid = input.getStringByField("user");
        String geneid = input.getStringByField("gene");
        log.info("user: " + userid + ",gene: " + geneid);
        Set<String> newGenes = new HashSet<String>();
        Set<String> oldGenes = getSetHbase(userid, FName.geneId.name(), geneActionTable);
        Set<String> result = new HashSet<String>();

        Collections.addAll(newGenes, geneid.split(","));
        // 更新用户基因表
        putGene(userid, geneid);

        // 基因删除
        result.addAll(oldGenes);
        result.removeAll(newGenes);
        log.info("delete gene:");
        for (String gene : result) {
            for (String classid : getSetHbase(gene, FName.classId.name(), tagInfo)) {
                putHbase(userid + "_" + classid, "0", "0");
//                putExWeight(userid + "_" + classid, "0", "0");
//                putClassWeight(userid + "_" + classid, "0", "0");
            }
        }
        result.clear();

        // 基因新增
        log.info("add gene:");
        result.addAll(newGenes);
        result.removeAll(oldGenes);
        for (String gene : result) {
            for (String classid : getSetHbase(gene, FName.classId.name(), tagInfo)) {
                addAction(userid, classid);
            }
        }
        result.clear();
        log.info("time cost in total(ms):" + (System.currentTimeMillis() - begin));
//        // 版面删除
//        log.info("delete page:");
//        for (String types : getPageClass(userid, "0")) {
//            Set<String> classes = getSetHbase(types, FName.classId.name(), pageClass);
//            for (String classid : fillClass(classes, userid)) {
////                putHbase(userid + "_" + classid, "0", "0");
////                putExWeight(userid + "_" + classid, "0", "0");
//                putClassWeight(userid + "_" + classid, "0", "0");
//            }
//        }
//
//        // 版面增加
//        log.info("add page:");
//        for (String types : getPageClass(userid, "1")) {
//            Set<String> classes = getSetHbase(types, FName.classId.name(), pageClass);
//            for (String classid : fillClass(classes, userid)) {
//                addAction(userid, classid);
//            }
//        }
    }

    // 获取基因分类表，版面分类表，版面热门分类表对应的分类集合
    private Set<String> getSetHbase(String id, String id_name, HTable table) {
        Set<String> userGene = new HashSet<String>();
        Get get = new Get(Bytes.toBytes(id));
        try {
            Result result = table.get(get);
            String gene;
            if (!result.isEmpty()) {
                gene = Bytes.toString(result.getValue(
                        Bytes.toBytes(FName.cf.name()), Bytes.toBytes(id_name)));
                log.info("gene: " + gene);
                String[] genes = gene.split(",");
                Collections.addAll(userGene, genes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userGene;
    }

//    // 获取对应表格对应字段的value
//    private String getHbase(String id, String id_name, HTable table) {
//        String weight = "0";
//        Get get = new Get(Bytes.toBytes(id));
//        try {
//            Result result = table.get(get);
//            weight = Bytes.toString(result.getValue(
//                    Bytes.toBytes(FName.cf.name()), Bytes.toBytes(id_name)));
//            if (weight == null) {
//                weight = "0";
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return weight;
//    }

    // 获取用户历史分类表中该用户的最大历史分类权重值
    private double maxHistory(String user, HTable table) {
        double maxhis = 0;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(user + "_"));
        scan.setStopRow(Bytes.toBytes(user + "_:"));
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (rs != null) {
            String weight;
            for (Result result : rs) {
                weight = Bytes.toString(result.getValue(
                        Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name())));
                maxhis = Math.max(maxhis, Double.parseDouble(weight));
            }
        }
        return maxhis;
    }

    // 更新用户基因表
    private void putGene(String user, String gene) {
        Put put = new Put(Bytes.toBytes(user));
        put.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.geneId.name()), Bytes.toBytes(gene));
        try {
            geneActionTable.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    // 基因增加操作
    private void addAction(String user, String classId) {
        String key = user + "_" + classId;
        double maxExcitation = Math.max(0.2, maxHistory(user, userClassHistory));
//        String userweight = getHbase(key, FName.weight.name(), userClassHistory);
//        double maxClassify = Math.max(maxExcitation,
//                userweight.isEmpty() ? 0 : Double.parseDouble(userweight));
        putHbase(key, "1", String.valueOf(maxExcitation));
//        putExWeight(key, "1", String.valueOf(maxExcitation));
//        putClassWeight(key, "1", String.valueOf(maxExcitation));
    }

    // 更新用户激发权重表
    private void putHbase(String key, String action, String weight) {
        Put exput = new Put(Bytes.toBytes(key));
        Put classput = new Put(Bytes.toBytes(key));
        exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.action.name()), Bytes.toBytes(action));
        exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()), Bytes.toBytes(weight));
        exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.latest_time.name()),
                Bytes.toBytes(TimeUtil.getTodayDate()));
        classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()), Bytes.toBytes(weight));
//        classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.action.name()), Bytes.toBytes(action));
//        classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.latest_time.name()),
//                Bytes.toBytes(TimeUtil.getTodayDate()));
        try {
            userExcitationWeight.put(exput);
            userClassidWeight.put(classput);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

//    // 更新用户激发权重表
//    private void putExWeight(String key, String action, String exweight) {
//        Put exput = new Put(Bytes.toBytes(key));
//        exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.action.name()), Bytes.toBytes(action));
//        exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()), Bytes.toBytes(exweight));
//        exput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.latest_time.name()),
//                Bytes.toBytes(TimeUtil.getTodayDate()));
//        try {
//            userExcitationWeight.put(exput);
//        } catch (InterruptedIOException e) {
//            e.printStackTrace();
//        } catch (RetriesExhaustedWithDetailsException e) {
//            e.printStackTrace();
//        }
//    }

    // 更新用户分类权重表
    private void putClassWeight(String key, String action, String classweight) {
        Put classput = new Put(Bytes.toBytes(key));
        classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.weight.name()), Bytes.toBytes(classweight));
        classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.action.name()), Bytes.toBytes(action));
        classput.add(Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.latest_time.name()),
                Bytes.toBytes(TimeUtil.getTodayDate()));
        try {
            userClassidWeight.put(classput);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    /**
     * 版面ID，单选，数字代表的含义如下：
     * 1   男生
     * 2   女生
     * 3   出版
     * 4   男生+出版
     * 5   女生+出版
     * 6   男生+女生
     * 7   男生+女生+出版
     * @param user 用户ID
     * @param action 增加1/删除0
     * @return 增加/删除的版面集合
     */
    private Set<String> getPageClass(String user, String action) {
        Set<String> page = new HashSet<String>();
        Set<String> newpage = new HashSet<String>();
        Set<String> oldpage = new HashSet<String>();
        Get get = new Get(Bytes.toBytes(user));
        try {
            Result result = userPageTable.get(get);
            if (!result.isEmpty()) {
                String pages = Bytes.toString(result.getValue(
                        Bytes.toBytes(FName.cf.name()), Bytes.toBytes(FName.req_edition.name())));
                log.info("left:" + pages.split("_")[0]);
                log.info("right: " + pages.split("_")[1]);
                String left = pages.split("_")[0];
                String right = pages.split("_")[1];
                oldpage = getPage(left);
                newpage = getPage(right);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (action.equals("1")) {
            page.addAll(newpage);
            page.removeAll(oldpage);
        } else {
            page.addAll(oldpage);
            page.removeAll(newpage);
        }
    log.info("page2: " + page);
        return page;
    }

    // 获取版面ID
    private Set<String> getPage(String type) {
        Set<String> page = new HashSet<String>();
        if (type.trim().equals("4")) {
            page.add("2");
            page.add("4");
        } else if (type.trim().equals("5")) {
            page.add("3");
            page.add("4");
        } else if (type.trim().equals("6")) {
            page.add("2");
            page.add("3");
        } else if (type.trim().equals("7")) {
            page.add("2");
            page.add("3");
            page.add("4");
        } else {
            page.add(String.valueOf(Integer.parseInt(type.trim()) + 1));
        }
        log.info("page1: " + page);
        return  page;
    }

    // 版面不足3个补白
    private Set<String> fillClass(Set<String> realtimeClass, String user) {
        for (String classId : getSetHbase(user, FName.bu_type.name(), pageHotClass)) {
            if (realtimeClass.size() >= 3) break;
            realtimeClass.add(classId);
        }
        return realtimeClass;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}