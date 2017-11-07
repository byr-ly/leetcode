package com.eb.bi.rs.mras.authorrec.itemcf.pre_author_filter;

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
import java.util.Collections;
import java.util.HashMap;

/**
 * Created by liyang on 2016/3/14.
 */
public class PreAuthorFilterReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, ArrayList<Author>> userAuthorMap;
    private ArrayList<Author> authorInfo;
    private HashMap<String, ArrayList<String>> authorTypeMap;
    private ArrayList<String> authorList;
    private ArrayList<Author> firstClassAuthor;
    private ArrayList<Author> secondClassAuthor;
    private ArrayList<Author> thirdClassAuthor;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        firstClassAuthor = new ArrayList<Author>();
        secondClassAuthor = new ArrayList<Author>();
        thirdClassAuthor = new ArrayList<Author>();
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            int i;
            for (i = 0; i < line.length; i++) {
                if (userAuthorMap.containsKey(key.toString()) && authorTypeMap.containsKey(line[i])) {
                    authorInfo = userAuthorMap.get(key.toString());
                    authorList = authorTypeMap.get(line[i]);
                    for (int j = 0; j < authorInfo.size(); j++) {
                        String authorName = authorInfo.get(j).name;
                        if (authorList.contains(authorName)) {
                            if (i == 0) {
                                firstClassAuthor.add(authorInfo.get(j));
                            } else if (i == 1) {
                                secondClassAuthor.add(authorInfo.get(j));
                            } else if (i == 2) {
                                thirdClassAuthor.add(authorInfo.get(j));
                            }
                        } else {
                            System.out.println("this author does not belong to current class!");
                        }
                    }
                } else {
                    System.out.println("can not get the filter result!");
                }
            }

            authorInfo.removeAll(firstClassAuthor);
            authorInfo.removeAll(secondClassAuthor);
            authorInfo.removeAll(thirdClassAuthor);


            Collections.sort(authorInfo, new SortByScore());
            Collections.sort(firstClassAuthor, new SortByScore());
            Collections.sort(secondClassAuthor, new SortByScore());
            Collections.sort(thirdClassAuthor, new SortByScore());

            StringBuffer firstAuthorResult = new StringBuffer();
            StringBuffer firstScoreResult = new StringBuffer();
            if (firstClassAuthor.size() >= 10) {
                for (int m = 0; m < 10; m++) {
                    firstAuthorResult.append(firstClassAuthor.get(m).name + "|");
                    firstScoreResult.append(firstClassAuthor.get(m).score + "|");
                }
                context.write(new Text(key + "|" + line[0] + "|" + firstAuthorResult + firstScoreResult + "1"), NullWritable.get());
            } else {
                for (int n = 0; n < firstClassAuthor.size(); n++) {
                    firstAuthorResult.append(firstClassAuthor.get(n).name + "|");
                    firstScoreResult.append(firstClassAuthor.get(n).score + "|");
                }
                ArrayList<Author> firstCopy = new ArrayList<Author>();
                for (int k = 0; k < (10 - firstClassAuthor.size()); k++) {
                    firstCopy.add(authorInfo.get(k));
                    firstAuthorResult.append(authorInfo.get(k).name + "|");
                    firstScoreResult.append(authorInfo.get(k).score + "|");
                }
                context.write(new Text(key + "|" + line[0] + "|" + firstAuthorResult + firstScoreResult + "1"), NullWritable.get());
                authorInfo.removeAll(firstCopy);
            }

            StringBuffer secondAuthorResult = new StringBuffer();
            StringBuffer secondScoreResult = new StringBuffer();
            if (secondClassAuthor.size() >= 10) {
                for (int m = 0; m < 10; m++) {
                    secondAuthorResult.append(secondClassAuthor.get(m).name + "|");
                    secondScoreResult.append(secondClassAuthor.get(m).score + "|");
                }
                context.write(new Text(key + "|" + line[1] + "|" + secondAuthorResult + secondScoreResult + "2"), NullWritable.get());
            } else {
                for (int n = 0; n < secondClassAuthor.size(); n++) {
                    secondAuthorResult.append(secondClassAuthor.get(n).name + "|");
                    secondScoreResult.append(secondClassAuthor.get(n).score + "|");
                }
                ArrayList<Author> secondCopy = new ArrayList<Author>();
                for (int k = 0; k < (10 - secondClassAuthor.size()); k++) {
                    secondCopy.add(authorInfo.get(k));
                    secondAuthorResult.append(authorInfo.get(k).name + "|");
                    secondScoreResult.append(authorInfo.get(k).score + "|");
                }
                context.write(new Text(key + "|" + line[1] + "|" + secondAuthorResult + secondScoreResult + "2"), NullWritable.get());
                authorInfo.removeAll(secondCopy);
            }

            StringBuffer thirdAuthorResult = new StringBuffer();
            StringBuffer thirdScoreResult = new StringBuffer();
            if (thirdClassAuthor.size() >= 10) {
                for (int m = 0; m < 10; m++) {
                    thirdAuthorResult.append(thirdClassAuthor.get(m).name + "|");
                    thirdScoreResult.append(thirdClassAuthor.get(m).score + "|");
                }
                context.write(new Text(key + "|" + line[2] + "|" + thirdAuthorResult + thirdScoreResult + "3"), NullWritable.get());
            } else {
                for (int n = 0; n < thirdClassAuthor.size(); n++) {
                    thirdAuthorResult.append(thirdClassAuthor.get(n).name + "|");
                    thirdScoreResult.append(thirdClassAuthor.get(n).score + "|");
                }
                for (int k = 0; k < (10 - thirdClassAuthor.size()); k++) {
                    thirdAuthorResult.append(authorInfo.get(k).name + "|");
                    thirdScoreResult.append(authorInfo.get(k).score + "|");
                }
                context.write(new Text(key + "|" + line[2] + "|" + thirdAuthorResult + thirdScoreResult + "3"), NullWritable.get());
            }

        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        userAuthorMap = new HashMap<String, ArrayList<Author>>();
        authorTypeMap = new HashMap<String, ArrayList<String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("pre")) {
                //用户 | 作家 | 预测分
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    }
                    String user = fields[0];
                    String writer = fields[1];
                    double score = Double.parseDouble(fields[2]);
                    if (!userAuthorMap.containsKey(user)) {
                        Author author = new Author(writer, score);
                        authorInfo = new ArrayList<Author>();
                        authorInfo.add(author);
                        userAuthorMap.put(user, authorInfo);
                    } else {
                        Author author = new Author(writer, score);
                        authorInfo.add(author);
                    }
                }
                br.close();
                System.out.println("用户作家预测分列表加载成功 " + userAuthorMap.size());
            } else {
                //作家 | 分类 | 本数
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    }
                    String writer = fields[0];
                    String type = fields[1];
                    if (!authorTypeMap.containsKey(type)) {
                        authorList = new ArrayList<String>();
                        authorList.add(writer);
                        authorTypeMap.put(type, authorList);
                    } else {
                        authorList.add(writer);
                    }
                }
                br.close();
                System.out.println("作家分类列表加载成功 " + authorTypeMap.size());
            }
        }
    }
}
