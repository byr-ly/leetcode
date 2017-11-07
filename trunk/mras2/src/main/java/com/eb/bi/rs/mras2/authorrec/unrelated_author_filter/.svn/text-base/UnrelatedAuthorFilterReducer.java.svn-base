package com.eb.bi.rs.mras2.authorrec.unrelated_author_filter;

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
import java.util.HashSet;

/**
 * Created by liyang on 2016/3/16.
 */
public class UnrelatedAuthorFilterReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashSet<String> userList;
    private HashMap<String, ArrayList<String>> authorTypeMap;
    private ArrayList<String> authorList;
    private ArrayList<String> allAuthorList;
    private ArrayList<String> firstAuthorList;
    private ArrayList<String> secondAuthorList;
    private ArrayList<String> thirdAuthorList;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            String[] line = val.toString().split("\\|");
            if (userList.contains(key.toString())) {
                continue;
            } else {
                for (int i = 0; i < line.length; i++) {
                    if (authorTypeMap.containsKey(line[i])) {
                        if (i == 0) {
                            firstAuthorList = authorTypeMap.get(line[i]);
                            allAuthorList.removeAll(firstAuthorList);
                        } else if (i == 1) {
                            secondAuthorList = authorTypeMap.get(line[i]);
                            allAuthorList.removeAll(secondAuthorList);
                        } else if (i == 2) {
                            thirdAuthorList = authorTypeMap.get(line[i]);
                            allAuthorList.removeAll(thirdAuthorList);
                        }
                    } else {
                        continue;
                    }
                }
            }

            StringBuffer firstAuthor = new StringBuffer();
            StringBuffer secondAuthor = new StringBuffer();
            StringBuffer thirdAuthor = new StringBuffer();
            StringBuffer fourthAuthor = new StringBuffer();
            ArrayList<String> filterAuthorList = new ArrayList<String>();
            if (firstAuthorList.size() >= 10) {
                for (int i = 0; i < 10; i++) {
                    firstAuthor.append(firstAuthorList.get(i) + "|");
                }
                context.write(new Text(key + "|" + line[0] + "|" + firstAuthor + "51"), NullWritable.get());
            } else {
                for (int j = 0; j < firstAuthorList.size(); j++) {
                    firstAuthor.append(firstAuthorList.get(j) + "|");
                }
                for (int k = 0; k < 10 - firstAuthorList.size(); k++) {
                    firstAuthor.append(allAuthorList.get(k) + "|");
                    filterAuthorList.add(allAuthorList.get(k));
                }
                allAuthorList.removeAll(filterAuthorList);
                filterAuthorList.clear();
                context.write(new Text(key + "|" + line[0] + "|" + firstAuthor + "51"), NullWritable.get());
            }

            if (secondAuthorList.size() >= 10) {
                for (int i = 0; i < 10; i++) {
                    secondAuthor.append(secondAuthorList.get(i) + "|");
                }
                context.write(new Text(key + "|" + line[1] + "|" + secondAuthor + "52"), NullWritable.get());
            } else {
                for (int j = 0; j < secondAuthorList.size(); j++) {
                    secondAuthor.append(secondAuthorList.get(j) + "|");
                }
                for (int k = 0; k < 10 - secondAuthorList.size(); k++) {
                    secondAuthor.append(allAuthorList.get(k) + "|");
                    filterAuthorList.add(allAuthorList.get(k));
                }
                allAuthorList.removeAll(filterAuthorList);
                filterAuthorList.clear();
                context.write(new Text(key + "|" + line[1] + "|" + secondAuthor + "52"), NullWritable.get());
            }

            if (thirdAuthorList.size() >= 10) {
                for (int i = 0; i < 10; i++) {
                    thirdAuthor.append(thirdAuthorList.get(i) + "|");
                }
                context.write(new Text(key + "|" + line[2] + "|" + thirdAuthor + "53"), NullWritable.get());
            } else {
                for (int j = 0; j < thirdAuthorList.size(); j++) {
                    thirdAuthor.append(thirdAuthorList.get(j) + "|");
                }
                for (int k = 0; k < 10 - thirdAuthorList.size(); k++) {
                    thirdAuthor.append(allAuthorList.get(k) + "|");
                    filterAuthorList.add(allAuthorList.get(k));
                }
                allAuthorList.removeAll(filterAuthorList);
                filterAuthorList.clear();
                context.write(new Text(key + "|" + line[2] + "|" + thirdAuthor + "53"), NullWritable.get());
            }

            if (allAuthorList.size() >= 10) {
                for (int m = 0; m < 10; m++) {
                    fourthAuthor.append(allAuthorList.get(m) + "|");
                }
                context.write(new Text(key + "|" + fourthAuthor + "54"), NullWritable.get());
            } else {
                for (int n = 0; n < allAuthorList.size(); n++) {
                    fourthAuthor.append(allAuthorList.get(n) + "|");
                }
                context.write(new Text(key + "|" + fourthAuthor + "54"), NullWritable.get());
            }
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        userList = new HashSet<String>();
        authorTypeMap = new HashMap<String, ArrayList<String>>();
        allAuthorList = new ArrayList<String>();

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
                //用户 | 预测作家
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 2) {
                        continue;
                    }
                    String user = fields[0];
                    if (!userList.contains(user)) {
                        userList.add(user);
                    } else {
                        continue;
                    }
                }
                br.close();
                System.out.println("预测作家列表加载成功 " + userList.size());
            } else {
                //作家 | 分类 | 分类下本数
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 3) {
                        continue;
                    }
                    String writer = fields[0];
                    String type = fields[1];
                    allAuthorList.add(writer);
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
