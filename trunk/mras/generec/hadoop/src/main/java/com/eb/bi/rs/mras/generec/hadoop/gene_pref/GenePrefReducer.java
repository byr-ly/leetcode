package com.eb.bi.rs.mras.generec.hadoop.gene_pref;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by liyang on 2016/6/27.
 */
public class GenePrefReducer extends Reducer<Text, Text, Text, NullWritable> {

    private HashMap<String, ArrayList<String>> geneTypeMap;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            ArrayList<String> similarTypeList = new ArrayList<String>();
            ArrayList<String> readTypeList = new ArrayList<String>();
            String[] line = val.toString().split("\\|");
            String firstTypePref = line[1];
            String secondTypePref = line[3];
            String thirdTypePref = line[5];
            String similarTypePref = line[7];
            String readTypePref = line[9];
            String class1 = line[0];
            String class2 = line[2];
            String class3 = line[4];
            String[] similarType = line[6].split(",");
            String[] readType = line[8].split(",");
            for (int i = 0; i < similarType.length; i++) {
                similarTypeList.add(similarType[i]);
            }
            for (int j = 0; j < readType.length; j++) {
                readTypeList.add(readType[j]);
            }

            double score = 0.0;
            Iterator iter = geneTypeMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String gene = (String) entry.getKey();
                ArrayList<String> typeList = geneTypeMap.get(gene);
                ArrayList<String> typeListCopyOne = new ArrayList<String>();
                ArrayList<String> typeListCopyTwo = new ArrayList<String>();

                if(!typeList.isEmpty()) typeListCopyOne.addAll(typeList);
                if(!typeList.isEmpty()) typeListCopyTwo.addAll(typeList);
                if (typeList.contains(class1)) score += Double.parseDouble(firstTypePref);
                if (typeList.contains(class2)) score += Double.parseDouble(secondTypePref);
                if (typeList.contains(class3)) score += Double.parseDouble(thirdTypePref);
                typeListCopyOne.retainAll(similarTypeList);
                if (typeListCopyOne.size() != 0) score += Double.parseDouble(similarTypePref);
                typeListCopyTwo.retainAll(readTypeList);
                if (typeListCopyTwo.size() != 0) score += Double.parseDouble(readTypePref);
                context.write(new Text(key.toString() + "|" + gene + "|" + score), NullWritable.get());
                score = 0.0;
            }
        }
    }

    protected void setup(Context context) throws IOException, InterruptedException {

        geneTypeMap = new HashMap<String, ArrayList<String>>();

        System.out.printf("reduce setup");
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        if (localFiles == null) {
            System.out.println("没有找到基因分类信息File ");
            return;
        }
        for (int i = 0; i < localFiles.length; i++) {
            System.out.println("localFile: " + localFiles[i]);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));

            String fileName = localFiles[i].toString();
            if (fileName.contains("gene")) {
                //基因 | 分类
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\\|");
                    if (fields.length < 2) {
                        continue;
                    }
                    String gene = fields[0];
                    String[] type = fields[1].split(",");
                    if (!geneTypeMap.containsKey(gene)) {
                        ArrayList<String> typeList = new ArrayList<String>();
                        for (int j = 0; j < type.length; j++) {
                            if (typeList.contains(type[j])) continue;
                            typeList.add(type[j]);
                        }
                        geneTypeMap.put(gene, typeList);
                    } else {
                        ArrayList<String> typeList = geneTypeMap.get(gene);
                        for (int j = 0; j < type.length; j++) {
                            if (typeList.contains(type[j])) continue;
                            typeList.add(type[j]);
                        }
                        geneTypeMap.put(gene, typeList);
                    }
                }
                br.close();
                System.out.println("基因分类列表加载成功" + geneTypeMap.size());
            }
        }
    }
}

