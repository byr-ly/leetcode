package com.eb.bi.rs.mras2.authorrec.author_pre_score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liyang on 2016/3/9.
 */
public class AuthorPreScoreReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, Double> authorAveScoreMap;
    private HashMap<String, HashMap<String, Double>> authorSimMap;
    private HashMap<String, Double> authorSim;
    private ArrayList<String> ratedAuthorList;
    private HashMap<String, HashMap<String, Double>> authorScoreMap;
    private HashMap<String, Double> userScore;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            //对于每一个预测作家，找到和他相似度大于0且被用户评过分的作家列表
            ratedAuthorList = new ArrayList<String>();
            String line = val.toString();
            if (authorSimMap.containsKey(line)) {
                authorSim = authorSimMap.get(line);
                Iterator iter = authorSim.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String writer = (String) entry.getKey();
                    Double sim = authorSim.get(writer);
                    if (authorScoreMap.containsKey(writer)) {
                        userScore = authorScoreMap.get(writer);
                        if (userScore.containsKey(key.toString()) && sim > 0) {
                            ratedAuthorList.add(writer);
                        } else {
                            System.out.println("the writer does not meet the requirements!");
                        }
                    }
                    System.out.println("the writer does not meet any user!");
                }
            } else {
                System.out.println("the writer does not have the similarity!");
            }

            //遍历已评分作家列表，计算未评分作家的预测得分
            Double preAuthorAveScore;
            if (authorAveScoreMap.containsKey(line)) {
                preAuthorAveScore = authorAveScoreMap.get(line);
            } else {
                preAuthorAveScore = 0.0;
            }
            double numerator = 0.0;
            double denominator = 0.0;
            Double simi;
            if(authorSimMap.containsKey(line)){
                authorSim = authorSimMap.get(line);
                for (int i = 0; i < ratedAuthorList.size(); i++) {
                    simi = authorSim.get(ratedAuthorList.get(i));
                    userScore = authorScoreMap.get(ratedAuthorList.get(i));
                    Double score = userScore.get(key.toString());
                    Double ratedAuthorAveScore = authorAveScoreMap.get(ratedAuthorList.get(i));
                    numerator += (simi * (score - ratedAuthorAveScore));
                    denominator += simi;
                }
                double preAuthorScore = preAuthorAveScore + numerator / denominator;
                context.write(new Text(key + "|" + line + "|" + preAuthorScore), NullWritable.get());
            }
            else{
                context.write(new Text(key + "|" + line + "|" + "0"),NullWritable.get());
            }
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        authorAveScoreMap = new HashMap<String, Double>();
        authorSimMap = new HashMap<String, HashMap<String, Double>>();
        ratedAuthorList = new ArrayList<String>();
        authorScoreMap = new HashMap<String, HashMap<String, Double>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("");
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("Ave")) {
                //作家  | 平均分  生成HashMap<作家，平均分>
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 2) {
                        continue;
                    } else {
                        String writer = fields[0];
                        if (!authorAveScoreMap.containsKey(writer)) {
                            authorAveScoreMap.put(writer, Double.parseDouble(fields[1]));
                        } else {
                            System.out.println("this record has been in this map");
                        }
                    }
                }
                br.close();
                System.out.println("作家平均分列表加载成功 " + authorAveScoreMap.size());
            } else if (fileName.contains("similarity")) {
                //作家 | 作家 | 相似度  生成HashMap<作家，HashMap<作家，相似度>>
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    } else {
                        String writerFront = fields[0];
                        String writerBack = fields[1];
                        Double similarity = Double.parseDouble(fields[2]);
                        if (!authorSimMap.containsKey(writerFront)) {
                            authorSim = new HashMap<String, Double>();
                            authorSim.put(writerBack, similarity);
                            authorSimMap.put(writerFront, authorSim);
                        } else {
                            authorSim = authorSimMap.get(writerFront);
                            authorSim.put(writerBack, similarity);
                        }
                    }
                }
                br.close();
                System.out.println("作家相似度列表加载成功" + authorSimMap.size());
            } else if (fileName.contains("score")) {
                //用户 | 作家 | 评分  生成HashMap<作家，HashMap<用户，评分>>
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    } else {
                        String user = fields[0];
                        String writer = fields[1];
                        Double score = Double.parseDouble(fields[2]);
                        if (!authorScoreMap.containsKey(writer)) {
                            userScore = new HashMap<String, Double>();
                            userScore.put(user, score);
                            authorScoreMap.put(writer, userScore);
                        } else {
                            userScore = authorScoreMap.get(writer);
                            userScore.put(user, score);
                        }
                    }
                }
                br.close();
                System.out.println("用户作家评分列表加载成功" + authorScoreMap.size());
            }
        }
    }
}
