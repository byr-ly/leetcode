package com.eb.bi.rs.mras.generec.storm.bolt;


import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mras.generec.storm.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 相似分
 *
 * @author wyb
 * @version 1.0
 * @date 创建时间：2015-11-24
 */
public class SimilarBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private static final long timeCell = 1000 * 60 * 60;
    private static final double ln2 = Math.log(2);
    private static double increaseScore;// 每增加一次被关联次数时所提高的相似评分
    private static int sampleNumber;// 相似分计算选取的用户所操作过的图书的数量
    private static double history_weight;// 历史图书列表中分数所占的权重
    private static double recent_weight;// 近期图书列表中分数所占的权重
    private static double realtime_weight;// 根据实时行为算出的分数所占的权重
    private static int pv_score;// 浏览行为得分
    private static int read_score;// 阅读行为得分
    private static int order_score;// 订购行为得分
    private static int pv_half_time;// 浏览行为得分半衰期
    private static int read_half_time;// 阅读行为得分半衰期
    private static int order_half_time;// 订购行为得分半衰期
    private static String isWrite;//开关，控制是否需要将相似分计算结果写入HBase
    private static String isGetTop;//开关，控制是否需要在计算相似分是否计算全部数据
    private Map<Long, Map<Long, Float>> correlation_similarity = new HashMap<Long, Map<Long, Float>>();
    private Map<Long, Map<Long, Float>> content_similarity = new HashMap<Long, Map<Long, Float>>();

    private HTableInterface userRealTable;// 实时图书行为表
    private HTableInterface userRecentTable;// 近期图书列表
    private HTableInterface userHisTable;// 历史图书行为表
    private HTableInterface correlationTable;// 图书关联相似度表
    private HTableInterface contentTable;// 图书内容相似度表
    private HTableInterface similarityScoreTable;// 存储计算出的图书相似分

    //for test
    //private HTableInterface similarScoreTest;// 存储计算出的图书相似分

    private int count;
    private double weight;
    private int hour;
    // 定时器
    private transient Thread loader = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        PrintHelper.print("enter the bolt...");

        PluginConfig appConfig = ConfigReader.getInstance().initConfig(
                stormConf.get("AppConfig").toString());

        increaseScore = Double.parseDouble(appConfig.getParam("increaseScore"));
        sampleNumber = Integer.parseInt(appConfig.getParam("sampleNumber"));
        pv_score = Integer.parseInt(appConfig.getParam("pv_score"));
        read_score = Integer.parseInt(appConfig.getParam("read_score"));
        order_score = Integer.parseInt(appConfig.getParam("order_score"));
        pv_half_time = Integer.parseInt(appConfig.getParam("pv_half_time"));
        read_half_time = Integer.parseInt(appConfig.getParam("read_half_time"));
        order_half_time = Integer.parseInt(appConfig.getParam("order_half_time"));
        weight = Double.parseDouble(appConfig.getParam("weight"));
        history_weight = Double.parseDouble(appConfig.getParam("history_weight"));
        recent_weight = Double.parseDouble(appConfig.getParam("recent_weight"));
        realtime_weight = Double.parseDouble(appConfig.getParam("realtime_weight"));
        isWrite = appConfig.getParam("isWrite");
        isGetTop = appConfig.getParam("isGetTop");

        hour = Integer.parseInt((appConfig.getParam("hour_similar")));

        // 初始化Hbase配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                appConfig.getParam("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",
                appConfig.getParam("hbase.zookeeper.property.clientPort"));
        HConnection con = null;
        while (con == null) {
            try {
                con = HConnectionManager.createConnection(conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户实时图书行为表
        while (userRealTable == null) {
            try {
                userRealTable = con.getTable(appConfig
                        .getParam("userRealTableName"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户历史图书行为及黑名单表
        while (userRecentTable == null) {
            try {
                userRecentTable = con.getTable(appConfig
                        .getParam("userRecentTableName"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 用户历史图书行为及黑名单表
        while (userHisTable == null) {
            try {
                userHisTable = con.getTable(appConfig
                        .getParam("userHisTableName"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书关联相似度表
        while (correlationTable == null) {
            try {
                correlationTable = con.getTable(appConfig
                        .getParam("correlationTableName"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书内容相似度表
        while (contentTable == null) {
            try {
                contentTable = con.getTable(appConfig
                        .getParam("contentTableName"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 图书相似分表
        while (similarityScoreTable == null) {
            try {
                similarityScoreTable = con.getTable(appConfig
                        .getParam("similarityScoreTable"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // fot test 图书相似分表
//        while (similarScoreTest == null) {
//            try {
//                similarScoreTest = con.getTable(appConfig
//                        .getParam("similarScoreTest"));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        // 定时器，每天12点从HDFS上读取图书基础分、编辑分
        if (loader == null) {
            loader = new Thread(new Runnable() {
                public void run() {
                    while (true) {
                        Long startTime = new Date().getTime();
                        Scan correlation_scan = new Scan();
                        Scan content_scan = new Scan();
                        try {
                            count = 0;
                            correlation_similarity.clear();
                            content_similarity.clear();
                            // 初始化关联相似度
                            ResultScanner correlation_rs = correlationTable
                                    .getScanner(correlation_scan);

                            correlation_similarity = getSimilar(correlation_rs);

                            // 初始化内容
                            ResultScanner content_rs = contentTable
                                    .getScanner(content_scan);
                            content_similarity = getSimilar(content_rs);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        Date currentTime = new Date();
                        PrintHelper.print("SimilarBolt : the similarity finishes loading , total : "
                                + count + " data ,consumed time : "
                                + (currentTime.getTime() - startTime));
                        long sleepTime = TimeUtil
                                .getMillisFromNowToTwelveOclock(hour);
                        if (sleepTime > 0) {
                            try {
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
            loader.setDaemon(true);
            loader.start();
        }
    }

    /*
     * input：user,edition
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        long startTime = new Date().getTime();
        String user = input.getStringByField("user");
        String gene = input.getStringByField("gene");
        PrintHelper.print("SimilarBolt receive userid:" + user + ">> gene_id:" + gene);

        try {
            Map<String, Double> hisBook_Scores = getOfflineBooksScore(user,
                    userHisTable, history_weight);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " hisBook_Scores:" + hisBook_Scores.size());

            Map<String, Double> recentBook_Scores = getOfflineBooksScore(user,
                    userRecentTable, recent_weight);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " recentBook_Scores:" + recentBook_Scores.size());

            Map<String, Double> realBebavior_Scores = new HashMap<String, Double>();
            Map<String, Double> similarbooks_Scores = new HashMap<String, Double>();
            Map<String, Double> assoBooks_Scores = new HashMap<String, Double>();
            Map<String, Double> contentBooks_Scores = new HashMap<String, Double>();

            // 获取实时行为表中所有该用户操作过的图书
            Map<String, Long> realBebaviorBooks = getUserRealBehavior(user);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " realBebaviorBooks:" + realBebaviorBooks.size());

            realBebavior_Scores = getRealBehaviorScore(realBebaviorBooks);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " realBebavior_Scores:" + realBebavior_Scores.size());

            Map<String, Double> book_Scores_map = mergeBooksScore(
                    realBebavior_Scores, recentBook_Scores, hisBook_Scores);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " book_Scores_map:" + book_Scores_map.size());

            assoBooks_Scores = getSimilarScore(book_Scores_map,
                    correlation_similarity);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " assoBooks_Scores:" + assoBooks_Scores.size());

            contentBooks_Scores = getSimilarScore(book_Scores_map,
                    content_similarity);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " contentBooks_Scores:" + contentBooks_Scores.size());

            similarbooks_Scores = mergeSimilarScore(assoBooks_Scores,
                    contentBooks_Scores);
            PrintHelper.print("MergeBolt receive user: " + user
                    + " similarbooks_Scores:" + similarbooks_Scores.size());



            //for test 直接获取hbase中的相似分结果
//            HashMap<String,Double> similarbooks_Scores = new HashMap<String, Double>();
//            Get get = new Get(Bytes.toBytes(user));
//            Result result1 = similarScoreTest.get(get);
//            String resultValue = Bytes.toString(result1.getValue(
//                    Bytes.toBytes("cf"), Bytes.toBytes("result")));
//            String[] bookScore = resultValue.split("\\|");
//            for(int i = 0; i < bookScore.length; i++){
//                if(bookScore[i].equals("")) continue;
//                String book = bookScore[i].split(",")[0];
//                Double score = Double.parseDouble(bookScore[i].split(",")[1]);
//                similarbooks_Scores.put(book,score);
//            }
//            PrintHelper.print("MergeBolt receive user: " + user
//                    + " similarbooks_Scores:" + similarbooks_Scores.size());

            Set<String> keys = similarbooks_Scores.keySet();
            Iterator<String> iter = keys.iterator();
            StringBuilder book_scores = new StringBuilder();

            while (iter.hasNext()) {

                String bookId = iter.next();
                Double score = similarbooks_Scores.get(bookId);
                book_scores.append(bookId + "," + score + "|");
            }
            String result = book_scores.toString();
            if ("true".equals(isWrite)) {
                addSimilarityScore(user, result);
            }
            Date currentTime = new Date();
            PrintHelper.print("SimilarBolt similarScore userid:" + user + " complete!!! consumed time : "
                    + (currentTime.getTime() - startTime));

            collector.emit(new Values(user, gene, result));

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "gene", "book_scores"));
    }

    /*
     * 获取用户实时数据
     */
    private Map<String, Long> getUserRealBehavior(String userId)
            throws Exception {
        Map<String, Long> realBebaviorBooks = new HashMap<String, Long>();
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(userId + "|"));
        scan.setStopRow(Bytes.toBytes(userId + "|A"));
        scan.setCaching(100);

        ResultScanner rs = userRealTable.getScanner(scan);

        for (Result result : rs) {

            String rowkey = Bytes.toString(result.getRow());
            String value = Bytes.toString(result.getValue("cf".getBytes(),
                    "time".getBytes()));
            if (value == null || value.length() < 14) {
                Exception e = new Exception("time format error！！");
                e.printStackTrace();
                continue;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Date date = sdf.parse(value);
            long time = date.getTime();
            realBebaviorBooks.put(rowkey, time);

        }
        return realBebaviorBooks;
    }

    /*
     * 根据用户实时行为对图书打分
     */
    private Map<String, Double> getRealBehaviorScore(Map<String, Long> map)
            throws Exception {
        Map<String, Double> realbook_scores = new HashMap<String, Double>();
        Set<String> set = map.keySet();
        Iterator<String> it = set.iterator();
        while (it.hasNext()) {
            String key = it.next();
            String[] u = key.split("\\|");
            if (u.length != 3 || u[1] == null || "".equals(u[1])) {
                continue;
            }
            // 获取用户最后一次对该图书采取该操作的时间
            long behaviorTime = map.get(key);

            Double score = 0.0;

            long time = new Date().getTime() - behaviorTime;

            int hours = (int) (time / timeCell);

            if ("pv".equals(u[2])) {
                score = pv_score * Math.exp(-(ln2 / pv_half_time) * hours);
            } else if ("read".equals(u[2])) {
                score = read_score * Math.exp(-(ln2 / read_half_time) * hours);
            } else if ("order".equals(u[2])) {
                score = order_score * Math.exp(-(ln2 / order_half_time) * hours);
            } else {
                Exception e = new Exception("the behavior : " + u[2]
                        + "is not exist");
                e.printStackTrace();
                continue;
            }
            if (score > 5) {
                score = 5.0;
            }
            if (realbook_scores.containsKey(u[1])) {
                if (score <= realbook_scores.get(u[1])) {
                    continue;
                }
            }
            realbook_scores.put(u[1], score * realtime_weight);
        }

        return realbook_scores;
    }

    /*
     * 获取用户号近期图书列表和历史图书列表的信息
     */
    private Map<String, Double> getOfflineBooksScore(String userId,
                                                     HTableInterface htable, double weight) throws Exception {

        Map<String, Double> map = new HashMap<String, Double>();

        Get get = new Get(Bytes.toBytes(userId));
        Result result = htable.get(get);
        String resultValue = Bytes.toString(result.getValue(
                Bytes.toBytes("cf"), Bytes.toBytes("result")));
        if (resultValue == null) {
            return map;
        }
        String[] book_scores = resultValue.split("\\|");

        for (String book_score : book_scores) {

            String[] bid_score = book_score.split(",");
            if (bid_score.length != 2 || bid_score[0] == null
                    || "".equals(bid_score[0])
                    || Double.parseDouble(bid_score[1]) < 0) {
                continue;
            }
            Double score = Double.parseDouble(bid_score[1]) * weight;
            map.put(bid_score[0], score);

        }

        return map;
    }

    /*
     * 获取相似度
     */
    private Map<String, Double> getSimilarScore(
            Map<String, Double> book_scores,
            Map<Long, Map<Long, Float>> similaritys) throws Exception {
        Map<Long, Double> map = new HashMap<Long, Double>();
        Map<Long, Integer> count = new HashMap<Long, Integer>();
        Set<String> set = book_scores.keySet();
        Iterator<String> it = set.iterator();

        while (it.hasNext()) {
            String bookId = it.next();
            Double score = book_scores.get(bookId);
            Map<Long, Float> resultValue = similaritys.get(Long
                    .parseLong(bookId));
            if (resultValue == null || "".equals(resultValue)) {
                PrintHelper.print("=====================:can't find similar books...");
                continue;
            }
            Set<Long> bid_set = resultValue.keySet();
            Iterator<Long> bid_it = bid_set.iterator();

            while (bid_it.hasNext()) {
                Long bid = bid_it.next();
                Float similar = resultValue.get(bid);

                double tempScore = similar * score;
                double similarScore = 0.0;
                if (map.containsKey(bid)) {
                    int n = count.get(bid);
                    n++;
                    count.put(bid, n);
                    similarScore = map.get(bid);
                    similarScore = tempScore > similarScore ? tempScore
                            : similarScore;
                } else {
                    count.put(bid, 0);
                    similarScore = tempScore;
                }
                map.put(bid, similarScore);
            }
        }
        return calculateSimilarScore(map, count);
    }

    /*
     * 获取相似度
     */
    private Map<Long, Map<Long, Float>> getSimilar(
            ResultScanner similarity_result) throws Exception {
        Map<Long, Map<Long, Float>> similarity_map = new HashMap<Long, Map<Long, Float>>();
        for (Result result : similarity_result) {
            Long bookId = Long.parseLong(Bytes.toString(result.getRow()));
            String resultValue = Bytes.toString(result.getValue(
                    Bytes.toBytes("cf"), Bytes.toBytes("similarity")));

            if (resultValue == null || "".equals(resultValue)) {
                continue;
            }
            Map<Long, Float> map = new HashMap<Long, Float>();
            String[] book_similaritys = resultValue.split("\\|");
            for (String book_similarity : book_similaritys) {

                String[] bid_similarity = book_similarity.split(",");
                if (bid_similarity.length != 2 || bid_similarity[0] == null
                        || "".equals(bid_similarity[0])) {
                    continue;
                }
                Float similar = 0.0f;
                if (bid_similarity[1].endsWith("%")) {

                    similar = Float.parseFloat(bid_similarity[1].substring(0,
                            bid_similarity[1].length() - 1)) / 100;
                } else {
                    similar = Float.parseFloat(bid_similarity[1]);
                }
                map.put(Long.parseLong(bid_similarity[0]), similar);
            }
            similarity_map.put(bookId, map);
            count++;
        }
        return similarity_map;
    }

    /*
     * 算多本图书推荐同一本书时，被推荐图书的相似分
     */
    private Map<String, Double> calculateSimilarScore(Map<Long, Double> map1,
                                                      Map<Long, Integer> map2) throws Exception {
        Map<String, Double> book_score = new HashMap<String, Double>();
        Set<Long> set = map1.keySet();
        Iterator<Long> it = set.iterator();

        while (it.hasNext()) {
            Long bookId = it.next();
            Double score = map1.get(bookId);
            double tempScore = score + map2.get(bookId) * increaseScore;
            score = tempScore >= 5 ? 5 : tempScore;
            book_score.put(String.valueOf(bookId), score);
        }

        return book_score;
    }

    /*
     * 合并相似分
     */
    private Map<String, Double> mergeSimilarScore(Map<String, Double> map1,
                                                  Map<String, Double> map2) throws Exception {

        Set<String> set = map1.keySet();
        Iterator<String> it = set.iterator();

        while (it.hasNext()) {
            String bookId = it.next();
            Double score1 = map1.get(bookId);
            double tempScore = 0.0;
            Double similarScore = 0.0;
            if (map2.containsKey(bookId)) {
                double score2 = map2.get(bookId);
                tempScore = score1 >= score2 ? (score1 + weight * score2)
                        : (0.1 * score1 + score2);
                similarScore = tempScore >= 5 ? 5 : tempScore;
            } else {
                similarScore = score1 >= 5 ? 5 : score1;
            }
            map2.put(bookId, similarScore);
        }

        return map2;
    }

    /*
     * 对用户实时行为评分、近期图书列表评分、历史图书列表评分评分进行合并
     */
    private Map<String, Double> mergeBooksScore(Map<String, Double> realMap,
                                                Map<String, Double> recentMap, Map<String, Double> hisMap)
            throws Exception {

        Map<String, Double> map = addMap(realMap, recentMap);
        map = addMap(map, hisMap);
        if ("true".equals(isGetTop)) {
            map = getTop(map);
        }
        return map;

    }

	/*
     * 合并两个Map
	 */

    private Map<String, Double> addMap(Map<String, Double> map1,
                                       Map<String, Double> map2) throws Exception {

        Set<String> keys2 = map2.keySet();

        Iterator<String> it = keys2.iterator();
        while (it.hasNext()) {
            String key = it.next();
            double value = map2.get(key);
            if (map1.containsKey(key)) {
                double score = map1.get(key);
                if (value <= score) {
                    continue;
                }
            }
            map1.put(key, value);
        }

        return map1;
    }

    /*
     * 取打分排名前N本书
     */
    private Map<String, Double> getTop(Map<String, Double> map)
            throws Exception {

        Collection<Double> scores = map.values();
        Double[] array = scores.toArray(new Double[scores.size()]);

        Arrays.sort(array);
        int n = array.length - sampleNumber;
        if (n < 0) {
            return map;
        }
        double standardScore = array[n];

        Map<String, Double> newmap = new HashMap<String, Double>();
        Set<String> set = map.keySet();
        Iterator<String> it = set.iterator();
        while (it.hasNext()) {

            String key = it.next();
            double value = map.get(key);
            if (value >= standardScore) {
                newmap.put(key, value);
            }

        }

        return newmap;
    }

    private void addSimilarityScore(String userId, String book_scores) {
        if (userId != null && book_scores != null && !"".equals(userId)
                && !"".equals(book_scores)) {

            Date date = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
            String rowKey = userId + "_" + formatter.format(date);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("similarScore"),
                    Bytes.toBytes(book_scores));
            try {
                similarityScoreTable.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
