package com.eb.bi.rs.mras.andnewsrec.load_allnews;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Created by LiMingji on 2016/3/18.
 */
public class LoadNewsReducer extends Reducer<Text, Text, Text, Text> {

    private static int N = 30;
    private HashMap<String,Double> wordIdfMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            this.N = Integer.parseInt(context.getConfiguration().get("wordsTopN", "30"));
        } catch (NumberFormatException e) {
            this.N = 30;
        }

        wordIdfMap = new HashMap<String, Double>();
        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到缓存信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("part")) {
                //word|idfValue
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|", -1);
                    if (fields.length < 2) {
                        continue;
                    }
                    String word = fields[0];
                    String idfValue = fields[1];
                    wordIdfMap.put(word,Double.parseDouble(idfValue));
                }
                br.close();
                System.out.println("词idf值列表加载成功 " + wordIdfMap.size());
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keyFields = key.toString().split("\\|", -1);
        if (keyFields.length < 2) {
            System.out.println("LoadNewsReducer bad line " + key.toString());
            return;
        }
        String newsID = keyFields[0];
        int totolWordNums = Integer.parseInt(keyFields[1]);

        ArrayList<WordWeight> allwordsWeight = new ArrayList<WordWeight>();
        for (Text value : values) {
            String[] allWords = value.toString().split("\\|", -1);
            for (String word : allWords) {
                String[] fields = word.split(",", -1);
                if (fields.length != 2) {
                    continue;
                }
                String splitWord = fields[0];
                int times = Integer.parseInt(fields[1]);
                double weight = 0.0;
                //计算词的TF-IDF值
                if(!wordIdfMap.containsKey(splitWord)) weight = 0.0;
                else{
                    double idfValue = wordIdfMap.get(splitWord);
                    double tf = (double)times / totolWordNums;
                    weight = tf * idfValue;
                }
                if (weight < 0.00001) {
                    continue;
                }
                allwordsWeight.add(new WordWeight(splitWord, weight));
            }
        }

        StringBuffer sb = new StringBuffer("");
        if (allwordsWeight.size() > N) {
            Collections.sort(allwordsWeight);
        }
        for (int i = 0; i < N && i < allwordsWeight.size(); i++) {
            sb.append(allwordsWeight.get(i).toString());
            sb.append("|");
        }
        context.write(new Text(newsID), new Text(sb.toString()));
    }
}


