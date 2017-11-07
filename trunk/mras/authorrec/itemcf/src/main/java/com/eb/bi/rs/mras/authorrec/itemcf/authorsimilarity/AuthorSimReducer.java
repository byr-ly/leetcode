package com.eb.bi.rs.mras.authorrec.itemcf.authorsimilarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by liyang on 2016/3/9.
 */
public class AuthorSimReducer extends Reducer<Text, Text, Text, NullWritable> {

    Logger log = Logger.getLogger(AuthorSimReducer.class);

    private HashMap<String, HashMap<String, String>> authorScoreMap;
    private ArrayList<String> commonUserList;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String authorPair[] = key.toString().split("\\|");
        for (Text val : values) {
            commonUserList = new ArrayList<String>();
            String[] line = val.toString().split("\\|");
            if (line.length >= 11) {
                for (int i = 0; i < line.length; i++) {
                    commonUserList.add(line[i]);
                }

                //遍历共同用户列表，找到每个用户对两个作家的评分
                double product = 0;
                double squareFront = 0.0;
                double squareBack = 0.0;
                for (int j = 0; j < commonUserList.size(); j++) {
                    if (authorScoreMap.containsKey(commonUserList.get(j))) {
                        HashMap scoreMap = authorScoreMap.get(commonUserList.get(j));
                        if (scoreMap.containsKey(authorPair[0]) && scoreMap.containsKey(authorPair[1])) {
                            double scoreFront = Double.parseDouble((String) scoreMap.get(authorPair[0]));
                            double scoreBack = Double.parseDouble((String) scoreMap.get(authorPair[1]));
                            product += scoreFront * scoreBack;
                            squareFront += Math.pow(scoreFront, 2);
                            squareBack += Math.pow(scoreBack, 2);
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                double similarity = product / (Math.sqrt(squareFront) * Math.sqrt(squareBack));
                context.write(new Text(key + "|" + similarity), NullWritable.get());
            } else {
                System.out.println("the common users are inadequate!");
            }
        }

    }

    protected void setup(Context context) throws IOException, InterruptedException {

        authorScoreMap = new HashMap<String, HashMap<String, String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到用户作家打分信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("score")) {
                //用户 | 作家 | 评分
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    }
                    String user = fields[0];
                    String writer = fields[1];
                    String score = fields[2];
                    if (!authorScoreMap.containsKey(user)) {
                        HashMap<String, String> authorScore = new HashMap<String, String>();
                        authorScore.put(writer, score);
                        authorScoreMap.put(user, authorScore);
                    } else {
                        HashMap<String, String> authorScore = authorScoreMap.get(user);
                        authorScore.put(writer, score);
                    }
                }
                br.close();
                System.out.println("用户作家打分列表加载成功 " + authorScoreMap.size());
            }
        }
    }
}
