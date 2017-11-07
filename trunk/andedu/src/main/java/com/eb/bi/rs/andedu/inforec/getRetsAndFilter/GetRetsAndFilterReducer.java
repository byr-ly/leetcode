package com.eb.bi.rs.andedu.inforec.getRetsAndFilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by LiMingji on 2016/3/21.
 */
public class GetRetsAndFilterReducer extends Reducer<Text, Text, Text, Text> {

    private static int N = 5;
//    private HashMap<String,String> newsTitleMap;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //Logger log = PluginUtil.getInstance().getLogger();
        Logger log = Logger.getLogger(GetRetsAndFilterReducer.class);
        try {
            N = Integer.parseInt(context.getConfiguration().get("top_news_N"));
        } catch (NumberFormatException e) {
            N = 5;
        }

//        newsTitleMap = new HashMap<String, String>();
//        System.out.printf("reduce setup");
//        Configuration conf = context.getConfiguration();
//        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
//
//        if (localFiles == null) {
//            System.out.println("没有找到缓存信息File ");
//            return;
//        }
//        for (int i = 0; i < localFiles.length; i++) {
//            System.out.println("localFile: " + localFiles[i]);
//            String line;
//            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
//
//            String fileName = localFiles[i].toString();
//            if (fileName.contains("0000")) {
//                //newsID classID title content
//                while ((line = br.readLine()) != null) {
//                    String[] fields = line.split("\u0001", -1);
//                    if (fields.length < 4) {
//                        continue;
//                    }
//                    String newsId = fields[0];
//                    String title = fields[2].replaceAll("[\\pP￥$^+~|<>\r\n]", " ");
//                    newsTitleMap.put(newsId, title);
//                }
//                br.close();
//                System.out.println("新闻类型列表加载成功 " + newsTitleMap.size());
//            }
//        }
    }

    /**
     * 输入数据
     * key:   newsID|classID
     * value: newsID|scores
     * 输出数据
     * key:   newsID
     * value  newsID,scores ....  （取TopN）
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> newsMap = new HashMap<String, Double>();
        HashMap<String, Double> newsModel = new HashMap<String, Double>();

        //String[] keyFields = key.toString().split("\\|");
        String newsID = key.toString();
//        String title = newsTitleMap.get(newsID);
        //String classID = keyFields[1];

        int count = 0;
        //获取相似新闻，并取TopN
        for (Text value : values) {
            String[] valueFields = value.toString().split("\\|");
            if (Double.parseDouble(valueFields[1]) < 1e-6 || Double.parseDouble(valueFields[2]) < 1e-6) {
                continue;
            }
            String newsID2 = valueFields[0];
            String score1 = valueFields[1];
            String score2 = valueFields[2];

            count += 1;
            if (count % 1000 == 0) {
                context.progress();
            }

            if (newsModel.containsKey(newsID)) {
                newsModel.put(newsID, newsModel.get(newsID) + Math.pow(Double.parseDouble(score1), 2));
            } else {
                newsModel.put(newsID, Math.pow(Double.parseDouble(score1), 2));
            }
            if (newsModel.containsKey(newsID2)) {
                newsModel.put(newsID2, newsModel.get(newsID2) + Math.pow(Double.parseDouble(score2), 2));
            } else {
                newsModel.put(newsID2, Math.pow(Double.parseDouble(score2), 2));
            }

            CompInfos compNews = new CompInfos(newsID2, score1, score2);
            if (newsMap.containsKey(compNews.newsID)) {
                double preScore = newsMap.get(compNews.newsID);
                compNews.scores += preScore;
                newsMap.put(compNews.newsID, compNews.scores);
            } else {
                newsMap.put(compNews.newsID, compNews.scores);
            }
        }

        ArrayList<CompInfos> sortList = new ArrayList<CompInfos>();
        for (Map.Entry<String, Double> entry : newsMap.entrySet()) {
            CompInfos compNews = new CompInfos(entry.getKey(), entry.getValue() /
                    (Math.sqrt(newsModel.get(newsID)) * Math.sqrt(newsModel.get(entry.getKey()))));
            sortList.add(compNews);
        }
        newsMap.clear();
        Collections.sort(sortList,new SortByScore());

        DecimalFormat dcm = new DecimalFormat("0.####");
        StringBuffer sb = new StringBuffer("");
        for (int i = 0; i < N && i < sortList.size(); i++) {
            sb.append(sortList.get(i).newsID);
            sb.append(",");
//            String titleSimi = newsTitleMap.get(sortList.get(i).newsID);
//            sb.append(titleSimi);
//            sb.append(",");
            sb.append(dcm.format(sortList.get(i).scores));
            sb.append("|");
        }

        context.write(new Text(newsID), new Text(sb.toString()));
    }
}